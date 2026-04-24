from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR
from coordinator import Coordinator
from dotenv import load_dotenv
import sys
import time
import threading

load_dotenv()

# Get consumer ID from command line
if len(sys.argv) < 2:
    print("❌ Usage: python consumer_distributed.py <consumer_id>")
    print("   Example: python consumer_distributed.py 0")
    sys.exit(1)

CONSUMER_ID = int(sys.argv[1])
NUM_CONSUMERS = 3

# Initialize coordinator
coordinator = Coordinator()

# Define ALL teams we're tracking
ALL_TEAMS = [
    'Arsenal', 'Bayern', 'Atletico', 'PSG', 
    'Chelsea', 'Liverpool', 'Manchester United', 
    'Real Madrid', 'Barcelona'
]

# Calculate which teams THIS consumer handles
my_teams = [
    team for team in ALL_TEAMS 
    if coordinator.get_consumer_for_team(team, NUM_CONSUMERS) == CONSUMER_ID
]

print(f"\n{'='*60}")
print(f"🚀 CONSUMER {CONSUMER_ID} STARTING")
print(f"{'='*60}")
print(f"📋 My assigned teams: {', '.join(my_teams)}")
print(f"{'='*60}\n")

# Register with coordinator
coordinator.register_consumer(CONSUMER_ID, my_teams)

# Stats
message_count = 0
post_count = 0
my_posts = 0

# Keywords for filtering
KEYWORDS = {
    'soccer': [
        'soccer', 'football', 'ucl', 'champions league', 'premier league',
        'arsenal', 'bayern', 'atletico', 'psg', 'chelsea', 'liverpool',
        'manchester', 'real madrid', 'barcelona', 'goal', 'match'
    ],
    'nba': ['nba', 'lakers', 'celtics', 'warriors', 'lebron', 'curry', 'basketball', 'playoffs'],
    'sports': ['game', 'win', 'score', 'team', 'player', 'championship']
}

def extract_teams(text):
    """Find which teams are mentioned in the text"""
    text_lower = text.lower()
    found_teams = []
    
    for team in ALL_TEAMS:
        if team.lower() in text_lower:
            found_teams.append(team)
    
    return found_teams

def categorize_post(text):
    """Categorize post into soccer/nba/sports"""
    text_lower = text.lower()
    categories = []
    
    for category, keywords in KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            categories.append(category)
    
    return categories

# Heartbeat thread - runs in background
def heartbeat_loop():
    """Send heartbeat every 10 seconds"""
    while True:
        coordinator.heartbeat(CONSUMER_ID)
        time.sleep(10)

# Start heartbeat thread
heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
heartbeat_thread.start()
print(f"❤️  Heartbeat thread started\n")

def on_message_handler(message):
    global message_count, post_count, my_posts
    
    message_count += 1
    
    # Print stats every 1000 messages
    if message_count % 1000 == 0:
        print(f"[C{CONSUMER_ID}] 📊 Messages: {message_count:,} | Total posts: {post_count:,} | My posts: {my_posts}")
    
    try:
        commit = parse_subscribe_repos_message(message)
        
        if not hasattr(commit, 'ops') or not commit.ops:
            return
        
        if not hasattr(commit, 'blocks') or not commit.blocks:
            return
        
        car = CAR.from_bytes(commit.blocks)
        
        for op in commit.ops:
            if op.action != 'create':
                continue
            
            if not op.path or 'app.bsky.feed.post' not in op.path:
                continue
            
            if not op.cid:
                continue
            
            record = car.blocks.get(op.cid)
            if not record:
                continue
            
            post_count += 1
            
            # Process the post
            if isinstance(record, dict) and 'text' in record and record['text']:
                text = record['text']
                categories = categorize_post(text)
                
                # Only process posts that match our keywords
                if categories:
                    # Extract team mentions
                    teams_mentioned = extract_teams(text)
                    
                    # Check if ANY mentioned team belongs to THIS consumer
                    my_team_mentions = [t for t in teams_mentioned if t in my_teams]
                    
                    # Only process if it's about OUR teams
                    if my_team_mentions:
                        my_posts += 1
                        
                        # Show first 10 posts this consumer processes
                        if my_posts <= 10:
                            print(f"\n[C{CONSUMER_ID}] ⚽ POST #{my_posts}")
                            print(f"   Teams: {my_team_mentions}")
                            print(f"   Categories: {categories}")
                            print(f"   Text: {text[:120]}...")
                
    except Exception as e:
        if message_count <= 10:
            print(f"[C{CONSUMER_ID}] ❌ Error: {e}")

def main():
    print(f"[C{CONSUMER_ID}] 🔥 Connecting to AT Protocol firehose...\n")
    
    client = FirehoseSubscribeReposClient()
    
    try:
        client.start(on_message_handler)
    except KeyboardInterrupt:
        print(f"\n[C{CONSUMER_ID}] 👋 Shutting down gracefully...")

if __name__ == "__main__":
    main()