# test_connections.py
import asyncio
import aiohttp
import os
from dotenv import load_dotenv
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig

# Load environment variables from .env file
load_dotenv()

async def test_plex_connection():
    """Test connection to Plex API"""
    headers = {
        'X-Plex-Connect-Api-Key': os.getenv('PLEX_API_KEY'),
        'X-Plex-Connect-Customer-Id': os.getenv('PLEX_CUSTOMER_ID'),
        'Content-Type': 'application/json'
    }
    
    url = "https://connect.plex.com/scheduling/v1/jobs"
    params = {'limit': 10}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                response_text = await response.text()
                if response.status == 200:
                    try:
                        data = await response.json(content_type=None)
                        # Check if response is a list or dict
                        if isinstance(data, list):
                            print(f"✅ Plex API connection successful! Found {len(data)} jobs")
                        elif isinstance(data, dict):
                            print(f"✅ Plex API connection successful! Found {len(data.get('data', []))} jobs")
                        else:
                            print(f"✅ Plex API connection successful! Response type: {type(data)}")
                        return True
                    except Exception as json_err:
                        print(f"✅ Plex API connected (status 200) but response parsing issue: {json_err}")
                        print(f"Response text: {response_text[:500]}...")
                        return True
                else:
                    print(f"❌ Plex API error: {response.status}")
                    print(f"Response: {response_text}")
                    return False
    except Exception as e:
        print(f"❌ Plex API connection failed: {e}")
        return False

def test_cdf_connection():
    """Test connection to CDF"""
    try:
        from cognite.client.credentials import OAuthClientCredentials
        
        # Create proper OAuth credentials object
        print(f"\nAttempting CDF connection with:")
        print(f"  Token URL: {os.getenv('CDF_TOKEN_URL')}")
        print(f"  Client ID: {os.getenv('CDF_CLIENT_ID')[:10]}...")
        print(f"  Base URL: {os.getenv('CDF_HOST')}")
        print(f"  Project: {os.getenv('CDF_PROJECT')}")
        
        # Try multiple scope formats to find the correct one
        scope_attempts = [
            "user_impersonation",  # The scope that Auth0 seems to be using
            "",  # Try without scope (Auth0 might have default audience)
            "https://westeurope-1.cognitedata.com/.default",
            "https://api.cognitedata.com/.default",
            f"{os.getenv('CDF_HOST')}/.default",
        ]
        
        success = False
        for scope in scope_attempts:
            try:
                print(f"\nTrying scope: {scope if scope else 'no scope/default audience'}")
                if scope:
                    creds = OAuthClientCredentials(
                        token_url=os.getenv('CDF_TOKEN_URL'),
                        client_id=os.getenv('CDF_CLIENT_ID'),
                        client_secret=os.getenv('CDF_CLIENT_SECRET'),
                        scopes=[scope] if ' ' not in scope else scope.split()
                    )
                else:
                    # Try without any scope - Auth0 might use default audience
                    creds = OAuthClientCredentials(
                        token_url=os.getenv('CDF_TOKEN_URL'),
                        client_id=os.getenv('CDF_CLIENT_ID'),
                        client_secret=os.getenv('CDF_CLIENT_SECRET'),
                        scopes=[]
                    )
                
                # Try to create client with this scope
                config = ClientConfig(
                    client_name="plex-test",
                    base_url=os.getenv('CDF_HOST'),
                    project=os.getenv('CDF_PROJECT'),
                    credentials=creds
                )
                
                client = CogniteClient(config)
                
                # Test the connection
                try:
                    assets = client.assets.list(limit=1)
                    print(f"✅ CDF connection successful with scope: {scope if scope else 'default'}!")
                    print(f"   Can access {client.config.project} project")
                    return True
                except Exception as api_err:
                    if "Error generating access token" not in str(api_err):
                        print(f"  Got token but API call failed: {api_err}")
                    else:
                        print(f"  Token generation failed: {str(api_err)[:100]}...")
                    
            except Exception as scope_err:
                print(f"  Error: {str(scope_err)[:100]}...")
                continue
        
        if not success:
            print("\n⚠️  Could not connect with any scope configuration.")
            print("  Please verify in Auth0 that:")
            print(f"  - Client ID {os.getenv('CDF_CLIENT_ID')[:10]}... is configured correctly")
            print("  - The client has been granted access to the CDF API")
            print("  - The audience/scope is properly configured")
            return False
        
    except Exception as e:
        print(f"❌ CDF connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    print("Testing connections...")
    print("-" * 50)
    
    plex_ok = await test_plex_connection()
    cdf_ok = test_cdf_connection()
    
    print("-" * 50)
    if plex_ok and cdf_ok:
        print("✅ All connections successful! Ready to run the extractor.")
    else:
        print("❌ Some connections failed. Please check your configuration.")

if __name__ == "__main__":
    asyncio.run(main())