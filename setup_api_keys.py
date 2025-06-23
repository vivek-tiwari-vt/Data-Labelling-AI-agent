#!/usr/bin/env python3
"""
Setup script for API keys in the Multi-Agent Text Labeling System
This script helps you configure your API keys securely.
"""

import os
import sys
from pathlib import Path

def get_env_file_path():
    """Get the path to the .env file"""
    return Path(__file__).parent / "backend" / ".env"

def read_env_file():
    """Read the current .env file"""
    env_file = get_env_file_path()
    if not env_file.exists():
        print("❌ .env file not found. Creating from .env.example...")
        example_file = Path(__file__).parent / "backend" / ".env.example"
        if example_file.exists():
            env_file.write_text(example_file.read_text())
            print("✅ Created .env file from .env.example")
        else:
            print("❌ .env.example not found either!")
            return None
    
    return env_file.read_text()

def write_env_file(content):
    """Write content to .env file"""
    env_file = get_env_file_path()
    env_file.write_text(content)
    print(f"✅ Updated {env_file}")

def update_env_var(content, var_name, new_value):
    """Update an environment variable in the content"""
    lines = content.split('\n')
    updated = False
    
    for i, line in enumerate(lines):
        if line.startswith(f"{var_name}="):
            lines[i] = f"{var_name}={new_value}"
            updated = True
            break
    
    if not updated:
        lines.append(f"{var_name}={new_value}")
    
    return '\n'.join(lines)

def setup_openrouter_keys():
    """Setup OpenRouter API keys"""
    print("\n🔧 Setting up OpenRouter API Keys")
    print("💡 You can add multiple keys for load balancing and fallback")
    print("💡 Press Enter to skip a key if you don't have it")
    
    keys = {}
    key_names = ["OPENROUTER_API_KEY", "OPENROUTER_API_KEY_1", "OPENROUTER_API_KEY_2", "OPENROUTER_API_KEY_3"]
    
    for i, key_name in enumerate(key_names):
        if i == 0:
            prompt = f"Enter your primary OpenRouter API key: "
        else:
            prompt = f"Enter additional OpenRouter API key #{i} (optional): "
        
        key = input(prompt).strip()
        if key:
            keys[key_name] = key
    
    return keys

def setup_gemini_keys():
    """Setup Gemini API keys"""
    print("\n🤖 Setting up Gemini API Keys")
    print("💡 You can add multiple keys for load balancing and fallback")
    print("💡 Press Enter to skip a key if you don't have it")
    
    keys = {}
    key_names = ["GEMINI_API_KEY", "GEMINI_API_KEY_1", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"]
    
    for i, key_name in enumerate(key_names):
        if i == 0:
            prompt = f"Enter your primary Gemini API key: "
        else:
            prompt = f"Enter additional Gemini API key #{i} (optional): "
        
        key = input(prompt).strip()
        if key:
            keys[key_name] = key
    
    return keys

def setup_openai_key():
    """Setup OpenAI API key"""
    print("\n🧠 Setting up OpenAI API Key (optional)")
    key = input("Enter your OpenAI API key (optional): ").strip()
    return {"OPENAI_API_KEY": key} if key else {}

def main():
    print("🚀 Multi-Agent Text Labeling System - API Key Setup")
    print("=" * 60)
    
    # Read current .env file
    content = read_env_file()
    if not content:
        return
    
    print("🔍 Current .env file loaded successfully")
    
    # Ask user what they want to do
    print("\nWhat would you like to do?")
    print("1. Setup OpenRouter API keys")
    print("2. Setup Gemini API keys")
    print("3. Setup OpenAI API key")
    print("4. Setup all keys")
    print("5. View current configuration (masked)")
    print("6. Exit")
    
    choice = input("\nEnter your choice (1-6): ").strip()
    
    if choice == "1":
        keys = setup_openrouter_keys()
    elif choice == "2":
        keys = setup_gemini_keys()
    elif choice == "3":
        keys = setup_openai_key()
    elif choice == "4":
        print("Setting up all API keys...")
        keys = {}
        keys.update(setup_openrouter_keys())
        keys.update(setup_gemini_keys())
        keys.update(setup_openai_key())
    elif choice == "5":
        show_current_config()
        return
    elif choice == "6":
        print("👋 Goodbye!")
        return
    else:
        print("❌ Invalid choice!")
        return
    
    # Update .env file
    if keys:
        for var_name, value in keys.items():
            content = update_env_var(content, var_name, value)
        
        write_env_file(content)
        print(f"\n✅ Successfully updated {len(keys)} API key(s)!")
        print("\n🔒 Security reminder:")
        print("   - Never commit your .env file to version control")
        print("   - Keep your API keys secure and don't share them")
        print("   - The .env file is already in .gitignore")
    else:
        print("ℹ️  No keys were updated.")

def show_current_config():
    """Show current configuration with masked keys"""
    try:
        import subprocess
        result = subprocess.run(
            ["curl", "-s", "http://localhost:8000/api/v1/ai/keys/validation"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            print("\n🔍 Current API Key Configuration:")
            for provider, info in data["api_keys"].items():
                print(f"  {provider.upper()}: {info['count']} key(s)")
                for key in info['keys']:
                    print(f"    - {key}")
        else:
            print("❌ Could not connect to API server. Make sure it's running.")
    except Exception as e:
        print(f"❌ Error checking configuration: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Setup cancelled by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1) 