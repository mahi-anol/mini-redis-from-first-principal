import time
import fnmatch
from collections import defaultdict,deque
from typing import Dict,Set,List,Optional,Any

class PubSubManager:
    """
    Manages pub/sub channels, subscriptions, and real time message routing. Fire and Forget.

    """
    def __init__(self):
        # Channel -> Set of client sockets
        self.channels:Dict[str,Set[Any]]=defaultdict(set) 

        #Client socket -> Set of subscribed channels
        self.client_subscriptions: Dict[Any,Set[str]]=defaultdict(set) # kono kisu na paile empty set return korbe. Thats the purpose of defaultdict.

        #Statistics
        self.total_messages_published=0
        self.total_subscriptions=0

    def subscribe(self,client,*channels)->List[tuple]:
        """
        Subscribe a client to one or more channels.
        Returnd a list of (channel,subscription_count) tuples.
        """
        results=[]

        for channel in channels:
            # check if client is not already subscribed to the channel, add subscription.
            if channel not in self.client_subscriptions[client]:
                self.channels[channel].add(client)
                self.client_subscriptions[client]=channel
                self.total_subscriptions+=1

            #   Return current subscription count for this client.
            subscription_count=len(self.client_subscriptions[client])
            results.append((channel,subscription_count))

        return results
    
    def unsubscribe(self,client,*channels)->List[tuple]:
        """
        Unsubscribe a client from channels.
        If no channelss specified, unsubscribe from all the channels.
        Return list of (channel,subscriber count) tuples.
        """
        results=[]

        # If no channels specified unsubscribe from all.
        if not channels:
            channels = list(self.client_subscriptions[client]) #set converted to list.

        
        for channel in channels:
            if channel in self.client_subscriptions[client]:
                # Remove subscription
                self.channels[channel].discard(client)
                self.client_subscriptions[client].discard(channel)
                self.total_subscriptions-=1

                #Clean up empty channels (good memory practice.)

                if not self.channels[channel]:
                    del self.channels[channel]

            # return current subscription count for this client
            subscription_count=len(self.client_subscriptions[client])+len(self.client_pattern_subscriptions[client])
            results.append((channel,subscription_count))
        
        return results
    
    def publish(self,channel: str, message:str)-> int:
        """
        Publish message to a channel (Fire and Forget).
        Message is immendiately sent to all currently connected subscribers.
        If no subscribers are connected, the messahe is lost.
        Return the number of clients that received the messages.
        """

        subscribers=self.channels.get(channel,set())

        if not subscribers:
            return 0
        
        # Create message tuple (type,channel,message)
        pub_message=('message',channel,message)

        #immediately send message to each subscriber

        delivery_count=0
        for client in subscribers.copy():
            try:
                #Format message according to Redis Protocol.
                from .response import array, bulk_string
                response=array([
                    bulk_string(pub_message[0]),
                    bulk_string(pub_message[1]),
                    bulk_string(pub_message[2]),
                    ])
                client.send(response)
                delivery_count+=1

            except Exception:
                # If send fails client is likely diconnected
                # Remove from subscribers and cleanup
                self._cleanup_client(client)
            
        self.total_messages_published+=1
        return delivery_count
    
    def has_pending_message(self,client) ->bool:
        """Check if client has pending messages  (always False for fire and forget model)"""
        return False

    def get_channels(self,pattern:Optional[str]=None)->List[str]:
        """
        Get list of active channels optionally filtered by patten.
        """

        channels=list(self.channels.keys())

        if pattern:
            channels=[ch for ch in channels if fnmatch.fnmatch(ch,pattern)]

        return sorted(channels)
    
    def is_client_subscribed(self,client)->bool:
        """check if client has any active subscriptions."""
        return (len(self.client_subscriptions[client]) > 0 or len(self.client_pattern_subscriptions[client])>0)
    
    def get_client_subscription_count(self,client)->bool:
        """get client subscription count."""
        return (len(self.client_subscriptions[client]) + len(self.client_pattern_subscriptions[client]))
    
    def cleanup_client(self,client):
        """Cleanup all data for a disconnected client."""
        self._cleanup_client(client)


    def _cleanup_client(self,client):
        """Internal method to cleanup client data."""

        for channel in list(self.client_subscriptions[client]):
            self.channels[channel].discard(client)
            if not self.channels[channel]:
                del self.channels

        #Remove from pattern subscriptions
        for pattern in list(self.client_pattern_subscription[client]):
            self.pattern_subscriptions[pattern].discard(client)
            if not self.patten_subscriptions[pattern]:
                del self.pattern_subscriptions[pattern]


        #clean client data.
        subscription_count=len(self.client_subscriptions[client])
        self.total_subscriptions-=subscription_count

        del self.client_subscriptions[client]
        del self.client_pattern_subscriptions[client]


    def get_stats(self)-> Dict[str,Any]:
        """Get pub/sub statistics."""
        return{
            'channels':len(self.channels),
            'patterns':len(self.pattern_subscriptions),
            'total_subscriptions':self.total_subscriptions,
            'total_message_published':self.total_messages_published,
            'active_clients': len(self.client_subscriptions)
        }    

