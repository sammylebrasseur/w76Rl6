from dataforseo_client import configuration as dfs_config, api_client as dfs_api_provider
from dataforseo_client.api.serp_api import SerpApi
from dataforseo_client.rest import ApiException
from dataforseo_client.models.serp_google_organic_live_advanced_request_info import SerpGoogleOrganicLiveAdvancedRequestInfo
from pprint import pprint
from modules.supabase_module import *
import time
import random

import re

PHONE_REGEX = re.compile(
    r'(?:\+?1[\s\-.]?)?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}'
)

def only_digits(s: str) -> str:
    return re.sub(r'\D', '', s or '')

def looks_masked(s: str) -> bool:
    # quick reject for snippets like 773485XXXX etc.
    return 'X' in (s or '').upper()

def us_format(digits: str) -> str:
    # format 10 or 11-digit (+1) to a standard string
    if len(digits) == 11 and digits.startswith('1'):
        digits = digits[1:]
    if len(digits) == 10:
        return f'{digits[0:3]}-{digits[3:6]}-{digits[6:10]}'
    return digits  # fallback
def _get(obj, key, default=None):
    # Works for both dicts and SDK objects
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)

def iter_serp_texts(api_response):
    """Yield text fields from DataForSEO SERP response likely to contain phones."""
    if not api_response:
        return

    tasks = _get(api_response, 'tasks') or []
    for t in tasks:
        results = _get(t, 'result') or []
        for res in results:
            items = _get(res, 'items') or []
            for item in items:
                # Base fields commonly carrying phones
                for field in ('title', 'description', 'breadcrumb', 'pre_snippet', 'extended_snippet'):
                    val = _get(item, field)
                    if val:
                        yield str(val)
                # Sometimes numbers appear inside the URL
                val = _get(item, 'url')
                if val:
                    yield str(val)
                # Links array (if present)
                links = _get(item, 'links') or []
                for link in links:
                    for field in ('title', 'snippet', 'description', 'url'):
                        val = _get(link, field)
                        if val:
                            yield str(val)

def extract_full_number_from_serp(api_response, prefix_digits: str) -> str | None:
    """Return the first 10-digit US number that starts with prefix_digits, or None."""
    prefix_digits = only_digits(prefix_digits)
    if not prefix_digits:
        return None

    candidates = []
    for text in iter_serp_texts(api_response):
        if looks_masked(text):
            # skip lines that include XXXX masking
            continue
        for raw in PHONE_REGEX.findall(text):
            digits = only_digits(raw)
            # allow leading country code
            if len(digits) == 11 and digits.startswith('1'):
                digits = digits[1:]
            if len(digits) == 10 and digits.startswith(prefix_digits):
                candidates.append(digits)

    # Prefer numbers that match the *longest* prefix you have (if you pass more than 6 digits sometimes)
    if candidates:
        # Deduplicate preserving order
        seen = set()
        dedup = []
        for d in candidates:
            if d not in seen:
                seen.add(d)
                dedup.append(d)
        return us_format(dedup[0])

    return None


def dataforseo_call(keyword: str):
    # Configure HTTP basic authorization: basicAuth
    configuration = dfs_config.Configuration(username=config.DATAFORSEO_USERNAME,password=config.DATAFORSEO_PASSWORD)
    
    with dfs_api_provider.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        serp_api = SerpApi(api_client)

        try:

            api_response = serp_api.google_organic_live_advanced([SerpGoogleOrganicLiveAdvancedRequestInfo(
                language_name="English",
                location_name="United States",
                keyword=keyword
            )])
            
            # pprint(api_response)
            return api_response
        
        except ApiException as e:
            print("Exception: %s\n" % e)

def dataforseo_call_requests(keyword: str,proxy_url, login, password):
    import requests, base64

    cred_b64 = base64.b64encode(f"{login}:{password}".encode()).decode()
    url = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
    headers = {"Authorization": f"Basic {cred_b64}", "Content-Type": "application/json"}
    payload = [{"language_code": "en", "location_code": 2840, "keyword": f"{keyword}"}]

    proxy_url = "http://wxlqgrzc:mfa8bm49obls@156.237.23.161:5562"
    proxies = {"http": proxy_url, "https": proxy_url}
    print('Requesting with proxies:', proxies)
    r = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=30)
    print("Status code:", r.status_code)
    try:
        data = r.json()
    except Exception:
        print("Raw response:", r.text)
        raise

    # Optional: sanity check
    # from pprint import pprint; pprint(data)
    return data




if __name__ == "__main__":

    while True:

        # GET CONTACT TO FULLY ENRICH
        contact = execute_query("""
            SELECT *
            FROM public.contacts
            WHERE enriched_full IS NULL
            AND array_length(c.phones, 1) >= 1
            ORDER BY c.id ASC
            LIMIT 1;
        """)
        if not contact:
            print("No more contacts to enrich.")
            break
        contact = contact[0]
        print(f'\nEnriching contact:\n{contact}')

        try:

            # FORMAT PHONE AND KEYWORD SEARCH
            phone_snippet = contact['phones'][0]['number'].replace('X','')
            keyword = f'{contact['first_name']} {contact['last_name']} {phone_snippet}'

            # MAKE SERP REQUEST
            print(f"\nSearching keyword: {keyword}")
            # proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['ip']}:{proxy['port']}"
            # api_response = dataforseo_call_requests(keyword, proxy_url, config.DATAFORSEO_USERNAME, config.DATAFORSEO_PASSWORD)
            api_response = dataforseo_call(keyword)
            pprint(api_response)
            if "Payment required" in str(api_response):
                print("Payment required error from DataForSEO API. Stopping.")
                break

            # EXTRACT FULL PHONE NUMBER
            full_number = extract_full_number_from_serp(api_response, phone_snippet)
            print("Matched full number:", full_number)
            phone_enriched = 'true' if full_number else 'false'

        except:

            phone_enriched = 'error'

        finally:

            if phone_enriched == 'true':
                execute_query(f"""
                    UPDATE public.contacts
                    SET enriched_full = '{phone_enriched}',
                    phone = '{full_number}'
                    WHERE id = {contact['id']};
                """)

            elif phone_enriched == 'false':
                execute_query(f"""
                    UPDATE public.contacts
                    SET enriched_full = '{phone_enriched}'
                    WHERE id = {contact['id']};
                """)

        input('wait')
        random_wait = random.randint(1, 5)
        time.sleep(random_wait)
