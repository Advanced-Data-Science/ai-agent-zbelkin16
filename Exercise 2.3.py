import requests

def get_public_holidays(country_code="US", year=2024):
    """
    Get public holidays for a specific country and year
    Uses Nager.Date API (free, no key required)
    """
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/{country_code}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an exception for bad status codes
        
        holidays = response.json()
        return holidays
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {country_code}: {e}")
        return None

# Test with 3 different countries
test_countries = ['US', 'FR', 'JP']
holiday_summary = {}

for country in test_countries:
    holidays = get_public_holidays(country)
    
    if holidays:
        print(f"\nPublic Holidays in {country} (2024):")
        for holiday in holidays:
            name = holiday.get('localName', holiday.get('name'))  # fallback if 'localName' missing
            date = holiday.get('date')
            print(f"- {date}: {name}")
        
        # Save count for summary
        holiday_summary[country] = len(holidays)
    else:
        holiday_summary[country] = 0

# Summary of holiday counts
print("\n📊 Holiday Count Summary (2024):")
for country, count in holiday_summary.items():
    print(f"{country}: {count} holidays")
