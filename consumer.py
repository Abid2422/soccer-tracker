from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR
from dotenv import load_dotenv

load_dotenv()

message_count = 0
post_count = 0
saved_count = 0

KEYWORDS = {
    'soccer': ['soccer', 'football', 'ucl', 'champions league', 'arsenal', 'bayern', 
               'atletico', 'psg', 'premier league', 'goal', 'match'],
    'nba': ['nba', 'lakers', 'celtics', 'warriors', 'lebron', 'curry', 'basketball', 'playoffs'],
    'sports': ['game', 'win', 'score', 'team', 'player', 'championship']
}

def categorize_post(text):
    """Determine which categories this post belongs to"""
    text_lower = text.lower()
    categories = []
    
    for category, keywords in KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            categories.append(category)
    
    return categories

def on_message_handler(message):
    global message_count, post_count, saved_count
    
    message_count += 1
    
    if message_count % 100 == 0:
        print(f"📊 Messages: {message_count} | Posts: {post_count} | Matches: {saved_count}")
    
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
            
            # Record is a DICT, not an object!
            if isinstance(record, dict) and 'text' in record and record['text']:
                text = record['text']
                categories = categorize_post(text)
                
                # Only show if it matches our keywords
                if categories:
                    saved_count += 1
                    
                    # Show first 20
                    if saved_count <= 20:
                        print(f"\n⚽ MATCH #{saved_count} [{', '.join(categories)}]:")
                        print(f"   {text[:150]}")
                
    except Exception as e:
        if message_count <= 10:
            print(f"Error: {type(e).__name__}: {e}")

def main():
    print("🔥 Multi-Sport Tracker (TEST MODE)...")
    print("📊 Tracking: Soccer, NBA, General Sports")
    print("Will show first 20 matches\n")
    
    client = FirehoseSubscribeReposClient()
    client.start(on_message_handler)

if __name__ == "__main__":
    main()