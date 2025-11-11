import sys
import bcrypt

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_hash.py <your-secret-key-here>")
        print("Example: python generate_hash.py my-super-secret-key-123")
        sys.exit(1)
    
    secret_key = sys.argv[1]
    
    key_bytes = secret_key.encode('utf-8')    
    hashed_bytes = bcrypt.hashpw(key_bytes, bcrypt.gensalt())
    hashed_key_str = hashed_bytes.decode('utf-8')
    
    print("\nYour secret key: ", secret_key)
    print("Your BCRYPT hash (copy this to your .env file):")
    print(hashed_key_str)