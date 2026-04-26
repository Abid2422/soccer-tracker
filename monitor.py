from coordinator import Coordinator
import time
from datetime import datetime

def main():
    coord = Coordinator()
    
    print("🔍 CONSUMER MONITOR")
    print("=" * 60)
    print("Checking consumer health every 5 seconds...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            print(f"\n⏰ {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 60)
            
            # Get all registered consumers
            consumers = coord.get_all_consumers()
            
            if not consumers:
                print("❌ No consumers registered")
            else:
                for consumer in consumers:
                    consumer_id = consumer['consumer_id']
                    teams = consumer['teams']
                    is_alive = coord.is_alive(consumer_id)
                    
                    status = "✅ ALIVE" if is_alive else "💀 DEAD"
                    print(f"Consumer {consumer_id}: {status}")
                    print(f"  Teams: {', '.join(teams)}")
                    
                    # Get checkpoint
                    checkpoint = coord.get_checkpoint(consumer_id)
                    if checkpoint:
                        print(f"  Checkpoint: {checkpoint}")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n👋 Monitor stopped")

if __name__ == '__main__':
    main()