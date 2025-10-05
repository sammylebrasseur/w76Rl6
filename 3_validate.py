import requests
import config
from modules.supabase_module import *
import traceback

while True:

    # GET CONTACT TO VALIDATE
    contact = execute_query("""
        SELECT *
        FROM public.contacts
        WHERE phone IS NOT NULL
        AND validated IS NULL
        ORDER BY id ASC
        LIMIT 1;
    """)
    if not contact:
        print("No more contacts to validate.")
        break
    contact = contact[0]
    try:

        # VALIDATE CONTACT
        print(f'\nValidating contact:\n{contact}')
        url = "https://api.clearoutphone.io/v1/phonenumber/validate"
        payload = f'{ "number": {contact["phone"]}, "country_code": "US" }'
        headers = {
            'Content-Type': "application/json",
            'Authorization': f"Bearer:{config.CLEAROUTPHONEIO_API_TOKEN}",
            }
        response = requests.request("POST", url, data=payload, headers=headers)
        response = response.json()
        
        # UPSERT VALIDATION RESULTS
        execute_query(f"""
            UPDATE public.contacts
            SET validated = 'true'
            phone_type = '{response['data']['line_type']}',
            phone_carrier = '{response['data']['carrier']}',
            WHERE id = {contact['id']};  
        """)

    except:
        
        traceback.print_exc()

        # UPSERT ERROR
        execute_query(f"""
            UPDATE public.contacts
            SET validated = 'error'
            WHERE id = {contact['id']};  
        """)
        