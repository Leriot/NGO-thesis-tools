"""
Configuration Diagnostic Tool
Helps identify issues with scraping configuration
"""

import yaml
import json
from pathlib import Path


def check_config():
    """Check configuration files for issues."""
    print("=" * 80)
    print("Configuration Diagnostic Tool")
    print("=" * 80)

    # Check scraping_rules.yaml
    print("\n1. Checking scraping_rules.yaml...")
    config_file = Path("config/scraping_rules.yaml")

    if not config_file.exists():
        print("  ✗ ERROR: config/scraping_rules.yaml not found")
        return

    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    print("  ✓ File loaded successfully")

    # Check url_exclusions
    print("\n2. Checking url_exclusions...")
    if 'url_exclusions' in config:
        exclusions = config['url_exclusions']
        print(f"  Found {len(exclusions)} exclusion patterns")

        non_strings = [p for p in exclusions if not isinstance(p, str)]
        if non_strings:
            print(f"  ✗ ERROR: Found {len(non_strings)} non-string patterns:")
            for p in non_strings:
                print(f"    - {type(p)}: {p}")
        else:
            print("  ✓ All exclusion patterns are strings")
            print(f"  Sample patterns: {exclusions[:5]}")
    else:
        print("  ✗ WARNING: url_exclusions not found in config")

    # Check priority_patterns
    print("\n3. Checking priority_patterns...")
    if 'priority_patterns' in config:
        priority_patterns = config['priority_patterns']
        print(f"  Found priority_patterns with keys: {list(priority_patterns.keys())}")

        for level in ['high', 'medium', 'low']:
            if level in priority_patterns:
                patterns = priority_patterns[level]
                print(f"\n  {level.upper()} priority:")
                print(f"    Count: {len(patterns)}")

                non_strings = [p for p in patterns if not isinstance(p, str)]
                if non_strings:
                    print(f"    ✗ ERROR: Found {len(non_strings)} non-string patterns:")
                    for p in non_strings:
                        print(f"      - {type(p)}: {p}")
                else:
                    print(f"    ✓ All patterns are strings")
                    print(f"    Sample: {patterns[:3]}")
            else:
                print(f"  ✗ WARNING: '{level}' priority not found")
    else:
        print("  ✗ WARNING: priority_patterns not found in config")

    # Check other important settings
    print("\n4. Checking crawl settings...")
    if 'crawl' in config:
        crawl = config['crawl']
        print(f"  max_depth: {crawl.get('max_depth', 'NOT SET')}")
        print(f"  max_pages_per_site: {crawl.get('max_pages_per_site', 'NOT SET')}")
        print(f"  respect_robots_txt: {crawl.get('respect_robots_txt', 'NOT SET')}")
        print(f"  follow_external_links: {crawl.get('follow_external_links', 'NOT SET')}")
    else:
        print("  ✗ WARNING: crawl settings not found")

    print("\n5. Checking rate limiting...")
    if 'rate_limiting' in config:
        rate = config['rate_limiting']
        print(f"  delay_between_requests: {rate.get('delay_between_requests', 'NOT SET')} seconds")
        print(f"  timeout: {rate.get('timeout', 'NOT SET')} seconds")
    else:
        print("  ✗ WARNING: rate_limiting not found")

    print("\n" + "=" * 80)
    print("Diagnostic complete!")
    print("=" * 80)


if __name__ == "__main__":
    check_config()
