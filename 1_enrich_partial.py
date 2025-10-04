from modules.supabase_module import *
import requests
import traceback
from modules.fields import url_clean
import time

def scrape_contacts_for_companies():
    while True:
        company = execute_query("""
            SELECT id, website_url FROM offer_x.companies
            WHERE employees_enriched_with_saleshandy IS NULL
            AND website_url IS NOT NULL
            ORDER BY id ASC
            LIMIT 1;
        """)
        if not company:
            print("No more companies to process.")
            break
        company = company[0]
        print(company)

        try:
            cookies = {
                'shreferer': 'https://www.saleshandy.com/',
                'sh_ulp': 'https://www.saleshandy.com/',
                '_pk_id.1.2362': '190a6eb6a92820a0.1753492828.',
                'intercom-device-id-g27xawt4': 'e9d2e770-2ff1-464e-aec5-b9ddab523851',
                'ajs_user_id': 'bAmDv0Q6pY',
                '_ga_X6HKQ79V48': 'GS2.1.s1753831590$o17$g1$t1753832144$j60$l0$h0',
                '_pk_id.1.f842': 'd2b90fc3a9e68d28.1756768139.',
                '__stripe_mid': '22f6ad9d-81a2-4136-a05b-666eded354032c89fb',
                '_clck': 'sgvhmj%5E2%5Efz3%5E0%5E2033',
                'ajs_anonymous_id': '9bff9be9-b8e2-414b-bbea-13b1c076d0d8',
                '_pk_ref.1.f842': '%5B%22%22%2C%22%22%2C1758403108%2C%22https%3A%2F%2Fwww.saleshandy.com%2F%22%5D',
                '_pk_ses.1.f842': '1',
                '_pk_ref.1.2362': '%5B%22%22%2C%22%22%2C1758403110%2C%22https%3A%2F%2Fwww.saleshandy.com%2F%22%5D',
                '_pk_ses.1.2362': '1',
                '_gid': 'GA1.2.460314262.1758403110',
                '__stripe_sid': 'a977bb52-94cd-4647-90b0-06e370b2e9bc38a222',
                '_gcl_au': '1.1.2100020381.1753492828.958431567.1758403180.1758403179',
                'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjI1NDgwOSwiaWF0IjoxNzU4NDAzMTkwLCJleHAiOjE3NTkwMDc5OTB9.DlLsglQ3gzV4Xy0hDH3Pf8SRjQuOC-IGcv4QA4RWHdM',
                '_ga_L3H9YTTZRE': 'GS2.1.s1758403107$o15$g1$t1758403217$j33$l0$h0',
                'mp_e3cf3162ba59594dc0092174fbc217fc_mixpanel': '%7B%22distinct_id%22%3A%22%24device%3A756652a3-c896-458c-92ab-9faaeef332f5%22%2C%22%24device_id%22%3A%22756652a3-c896-458c-92ab-9faaeef332f5%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D',
                'mp_253dbcc3c9cbac771923a0687bd233d4_mixpanel': '%7B%22distinct_id%22%3A%22%24device%3A73be66e8-0e93-460f-b638-2efee77c1bad%22%2C%22%24device_id%22%3A%2273be66e8-0e93-460f-b638-2efee77c1bad%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%2C%22%24search_engine%22%3A%22google%22%7D',
                '_ga': 'GA1.2.1392090642.1753492828',
                '_ga_65VZV193TN': 'GS2.2.s1758403110$o24$g1$t1758403226$j60$l0$h0',
                'intercom-session-g27xawt4': 'UmlvK3hxejlkQURTRXphWFRQQ3Y2Mi9RTjdraGNxUlhobW80V1MzUlZyeUpJVldqVDJYYnNKQlNQQzdUNm9BQWo3K3lPSGM5WENWZGVtMGJRZFFqOEl2MFcrRnd3c05sOVFPZFphOG45RTA9LS0xM1VWbFA0cnlMbVcwK2pSbjhHZEhBPT0=--74bd2565ecd6b1b305f7f53a53936a10036735f3',
                'ph_phc_PTMqUe5mtfed0MbMlxkxzswdBrkx5gmw4Yrqd7AYEf1_posthog': '%7B%22distinct_id%22%3A%2201984451-317e-7f91-bad2-c5c52b02336a%22%2C%22%24sesid%22%3A%5B1758403462584%2C%22019968fe-2651-7742-bb3e-e501565f3c87%22%2C1758403110481%5D%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22u%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%7D%7D',
            }

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjI1NDgwOSwiaWF0IjoxNzU4NDAzMTkwLCJleHAiOjE3NTkwMDc5OTB9.DlLsglQ3gzV4Xy0hDH3Pf8SRjQuOC-IGcv4QA4RWHdM',
                'content-type': 'application/json;charset=UTF-8',
                'origin': 'https://my.saleshandy.com',
                'priority': 'u=1, i',
                'referer': 'https://my.saleshandy.com/leads?start=1&take=100&employer=lemonlight.com&managementLevels=Founder%2FOwner&managementLevels=C-Level&managementLevels=Vice+President&managementLevels=Head&managementLevels=Director&department=CU000&department=CU001&department=CU006&department=CU007&department=CU009&department=MR000&department=MR004&department=MR005&department=MR007&department=MR010',
                'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
                # 'cookie': 'shreferer=https://www.saleshandy.com/; sh_ulp=https://www.saleshandy.com/; _pk_id.1.2362=190a6eb6a92820a0.1753492828.; intercom-device-id-g27xawt4=e9d2e770-2ff1-464e-aec5-b9ddab523851; ajs_user_id=bAmDv0Q6pY; _ga_X6HKQ79V48=GS2.1.s1753831590$o17$g1$t1753832144$j60$l0$h0; _pk_id.1.f842=d2b90fc3a9e68d28.1756768139.; __stripe_mid=22f6ad9d-81a2-4136-a05b-666eded354032c89fb; _clck=sgvhmj%5E2%5Efz3%5E0%5E2033; ajs_anonymous_id=9bff9be9-b8e2-414b-bbea-13b1c076d0d8; _pk_ref.1.f842=%5B%22%22%2C%22%22%2C1758403108%2C%22https%3A%2F%2Fwww.saleshandy.com%2F%22%5D; _pk_ses.1.f842=1; _pk_ref.1.2362=%5B%22%22%2C%22%22%2C1758403110%2C%22https%3A%2F%2Fwww.saleshandy.com%2F%22%5D; _pk_ses.1.2362=1; _gid=GA1.2.460314262.1758403110; __stripe_sid=a977bb52-94cd-4647-90b0-06e370b2e9bc38a222; _gcl_au=1.1.2100020381.1753492828.958431567.1758403180.1758403179; token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjI1NDgwOSwiaWF0IjoxNzU4NDAzMTkwLCJleHAiOjE3NTkwMDc5OTB9.DlLsglQ3gzV4Xy0hDH3Pf8SRjQuOC-IGcv4QA4RWHdM; _ga_L3H9YTTZRE=GS2.1.s1758403107$o15$g1$t1758403217$j33$l0$h0; mp_e3cf3162ba59594dc0092174fbc217fc_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A756652a3-c896-458c-92ab-9faaeef332f5%22%2C%22%24device_id%22%3A%22756652a3-c896-458c-92ab-9faaeef332f5%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; mp_253dbcc3c9cbac771923a0687bd233d4_mixpanel=%7B%22distinct_id%22%3A%22%24device%3A73be66e8-0e93-460f-b638-2efee77c1bad%22%2C%22%24device_id%22%3A%2273be66e8-0e93-460f-b638-2efee77c1bad%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.saleshandy.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%2C%22%24search_engine%22%3A%22google%22%7D; _ga=GA1.2.1392090642.1753492828; _ga_65VZV193TN=GS2.2.s1758403110$o24$g1$t1758403226$j60$l0$h0; intercom-session-g27xawt4=UmlvK3hxejlkQURTRXphWFRQQ3Y2Mi9RTjdraGNxUlhobW80V1MzUlZyeUpJVldqVDJYYnNKQlNQQzdUNm9BQWo3K3lPSGM5WENWZGVtMGJRZFFqOEl2MFcrRnd3c05sOVFPZFphOG45RTA9LS0xM1VWbFA0cnlMbVcwK2pSbjhHZEhBPT0=--74bd2565ecd6b1b305f7f53a53936a10036735f3; ph_phc_PTMqUe5mtfed0MbMlxkxzswdBrkx5gmw4Yrqd7AYEf1_posthog=%7B%22distinct_id%22%3A%2201984451-317e-7f91-bad2-c5c52b02336a%22%2C%22%24sesid%22%3A%5B1758403462584%2C%22019968fe-2651-7742-bb3e-e501565f3c87%22%2C1758403110481%5D%2C%22%24initial_person_info%22%3A%7B%22r%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%2C%22u%22%3A%22https%3A%2F%2Fwww.saleshandy.com%2F%22%7D%7D',
            }

            json_data = {
                'start': 1,
                'take': 100,
                'employer': [
                    f'{company["website_url"]}'
                ],
                'managementLevels': [
                    'Founder/Owner',
                    'C-Level',
                    'Vice President',
                    'Head',
                    'Director',
                ],
                'department': [
                    'Executive',
                    'Founder',
                    'Marketing Executive',
                    'Operations Executive',
                    'Sales Executive',
                    'Advertising',
                    'Demand Generation',
                    'Digital Marketing',
                    'Product Marketing',
                    'Social Media Marketing',
                ],
            }

            while True:
                response = requests.post(
                    'https://my.saleshandy.com/api/edge/lead-finder/leads/search',
                    cookies=cookies,
                    headers=headers,
                    json=json_data,
                )
                if response.status_code != 201:
                    print('Rate limited. Sleeping for 60 seconds...')
                    time.sleep(60)
                else:
                    break
            profiles = response.json().get('payload', []).get('profiles', [])
            contacts = []
            for profile in profiles:
                contact = {}
                contact['company_id'] = company['id']
                try:
                    contact['first_name'] = profile['first_name']
                except:
                    pass
                try:
                    contact['last_name'] = profile['last_name']
                except:
                    pass
                try:
                    contact['saleshandy_profile_pic'] = profile['profile_pic']
                except:
                    pass
                try:
                    contact['linkedin_url'] = url_clean(profile['linkedin_url'])
                except:
                    pass
                try:
                    contact['city'] = profile['city']
                except:
                    pass
                try:
                    contact['state'] = profile['region']
                except:
                    pass
                try:
                    contact['country'] = profile['country']
                except:
                    pass
                try:
                    contact['title'] = profile['current_title']
                except:
                    pass
                try:
                    contact['professional_email_domains'] = profile['teaser']['professional_emails']
                except:
                    pass
                try:
                    contact['phones'] = profile['teaser']['phones']
                except:
                    pass

                contacts.append(contact)
            supabase.schema('offer_x').table('contacts').upsert(contacts, on_conflict='linkedin_url').execute()
            scraped = 'true'
        except:
            traceback.print_exc()
            scraped = 'error'
        finally:
            execute_query(f"""
                UPDATE offer_x.companies
                SET employees_enriched_with_saleshandy = '{scraped}'
                WHERE id = {company['id']};
            """)

        # input('wait')