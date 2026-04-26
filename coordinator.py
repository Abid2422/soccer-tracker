import redis
import hashlib
import json
from datetime import datetime

class Coordinator:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
    def get_consumer_for_team(self, team_name, num_consumers=3):
        """Hash team name to a consumer ID (0, 1, 2)"""
        team_hash = int(hashlib.md5(team_name.encode()).hexdigest(), 16)
        return team_hash % num_consumers
    
    def register_consumer(self, consumer_id, teams):
        """Register this consumer and its assigned teams"""
        data = {
            'consumer_id': consumer_id,
            'teams': teams,
            'started_at': datetime.utcnow().isoformat(),
            'last_heartbeat': datetime.utcnow().isoformat()
        }
        self.redis.set(f'consumer:{consumer_id}', json.dumps(data))
        print(f"✅ Consumer {consumer_id} registered with teams: {teams}")
    # Add these methods to the Coordinator class:

    def save_checkpoint(self, consumer_id, event_id):
        """Save the last processed event for this consumer"""
        self.redis.set(f'checkpoint:{consumer_id}', event_id)
        
    def get_checkpoint(self, consumer_id):
        """Get the last processed event for this consumer"""
        checkpoint = self.redis.get(f'checkpoint:{consumer_id}')
        return checkpoint if checkpoint else None

    def clear_checkpoint(self, consumer_id):
        """Clear checkpoint (for testing)"""
        self.redis.delete(f'checkpoint:{consumer_id}')
    
    def heartbeat(self, consumer_id):
        """Update heartbeat for this consumer"""
        consumer_data = self.redis.get(f'consumer:{consumer_id}')
        if consumer_data:
            data = json.loads(consumer_data)
            data['last_heartbeat'] = datetime.utcnow().isoformat()
            self.redis.set(f'consumer:{consumer_id}', json.dumps(data))
            # Also set expiring key for monitoring
            self.redis.setex(f'heartbeat:{consumer_id}', 30, 'alive')
    
    def is_alive(self, consumer_id):
        """Check if consumer is alive"""
        return self.redis.exists(f'heartbeat:{consumer_id}')
    
    def get_all_consumers(self):
        """Get info about all registered consumers"""
        keys = self.redis.keys('consumer:*')
        consumers = []
        for key in keys:
            data = self.redis.get(key)
            if data:
                consumers.append(json.loads(data))
        return consumers

# Test code
if __name__ == '__main__':
    coord = Coordinator()
    
    # Define our teams
    teams = ['Arsenal', 'Bayern', 'Atletico', 'PSG', 'Chelsea', 'Liverpool', 
             'Manchester United', 'Real Madrid', 'Barcelona']
    
    # Show partitioning
    print("\n🎯 TEAM PARTITIONING:")
    for team in teams:
        consumer = coord.get_consumer_for_team(team, num_consumers=3)
        print(f"  {team:20} → Consumer {consumer}")
    
    # Test registration
    print("\n📝 TESTING REGISTRATION:")
    coord.register_consumer(0, ['Arsenal', 'PSG'])
    coord.register_consumer(1, ['Bayern', 'Chelsea'])
    coord.register_consumer(2, ['Atletico', 'Liverpool'])
    
    # Test heartbeat
    print("\n❤️  TESTING HEARTBEAT:")
    coord.heartbeat(0)
    print(f"  Consumer 0 alive: {coord.is_alive(0)}")

    # Test checkpointing
    print("\n💾 TESTING CHECKPOINTING:")
    coord.save_checkpoint(0, 'event_12345')
    checkpoint = coord.get_checkpoint(0)
    print(f"  Consumer 0 checkpoint: {checkpoint}")
    coord.clear_checkpoint(0)
    print(f"  After clear: {coord.get_checkpoint(0)}")
    
    print("\n✅ All tests passed!")