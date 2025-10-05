from modules.supabase_module import *
import requests
import traceback
from modules.fields import *
import time
import config

def scrape_contacts_for_companies():

    while True:

        # GET CONTACTS TO PARTIALLY ENRICH
        db_contacts = execute_query("""
            SELECT * FROM public.contacts
            WHERE enriched_partial IS NULL
            ORDER BY id ASC
            LIMIT 100;
        """)
        if not db_contacts:
            print("No more companies to process.")
            break

        linkedin_urls = [full_url(c['linkedin_url']) for c in db_contacts if c['linkedin_url']]

        try:

            # MAKE REQUEST (WITH RATE LIMIT HANDLING)
            while True:
                response = requests.post(
                    'https://my.saleshandy.com/api/edge/lead-finder/leads/search',
                    cookies=config.SALESHANDY_COOKIES,
                    headers=config.SALESHANDY_HEADERS,
                    json={
                        'start': 1,
                        'take': 100,
                        'link': linkedin_urls,
                    },
                )
                if response.status_code != 201:
                    print('Rate limited. Sleeping for 60 seconds...')
                    time.sleep(60)
                else:
                    break

            # PARSE RESPONSE
            profiles = response.json().get('payload', []).get('profiles', [])
            contacts = []
            for profile in profiles:
                contact = {}
                try:
                    contact['phones'] = profile['teaser']['phones']
                except:
                    pass
                contacts.append(contact)

            # UPSERT CONTACT INFO
            for contact in contacts:
                contact['enriched_partial'] = 'true'
            supabase.schema('offer_x').table('contacts').upsert(contacts, on_conflict='linkedin_url').execute()

        except:

            traceback.print_exc()
            contact_ids = [c['id'] for c in db_contacts]
            supabase.schema('offer_x').table('contacts').update({'enriched_partial': 'error'}).in_('id', contact_ids).execute()


            

        # input('wait')