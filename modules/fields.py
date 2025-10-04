import tldextract
import re

def url_clean(website_url=str):
    
    if not website_url:
        return None
    
    cleaned_url = website_url
    if 'https://' in cleaned_url:
        cleaned_url = cleaned_url.replace('https://','')
    else: 
        pass
    if 'http://' in cleaned_url:
        cleaned_url = cleaned_url.replace('http://','')
    else: 
        pass
    if 'www.' in cleaned_url:
        cleaned_url = cleaned_url.replace('www.','')
    else:
        pass
    if cleaned_url.endswith('/'):
        cleaned_url = cleaned_url[:-1]
    return cleaned_url

def clean_domain(url):
    if not url:
        return None
    ext = tldextract.extract(url)
    if ext.domain and ext.suffix:
        return f"{ext.domain}.{ext.suffix}"
    return None

def clean_company_name(company_name=str):

    if not company_name:
        return None

    cleaned_company_name = company_name
    cleaned_company_name = re.sub(r'\s+', ' ', cleaned_company_name).strip()

    if "(" and ")" in cleaned_company_name:
        cleaned_company_name = re.sub(r'\s*\([^)]*\)', '', cleaned_company_name)
    if "[" and "]" in cleaned_company_name:
        cleaned_company_name = re.sub(r'\s*\[[^)]*\]', '', cleaned_company_name)
    if ", " in cleaned_company_name:
        cleaned_company_name = cleaned_company_name.split(',',1)[0].strip()
    if " - " in cleaned_company_name:
        cleaned_company_name = cleaned_company_name.split('-',1)[0].strip()
    if " | " in cleaned_company_name:
        cleaned_company_name = cleaned_company_name.split('|',1)[0].strip()

    def clean_appendage_strings(appendage_string=str, cleaned_company_name=str):
        if f", {appendage_string}." in cleaned_company_name.lower():
            cleaned_company_name = re.sub(rf'\s*, {appendage_string}.', '', cleaned_company_name, flags=re.IGNORECASE)
        if f", {appendage_string}" in cleaned_company_name.lower():
            cleaned_company_name = re.sub(rf'\s*, {appendage_string}', '', cleaned_company_name, flags=re.IGNORECASE)
        if f"{appendage_string}." in cleaned_company_name.lower():
            cleaned_company_name = re.sub(rf'\s*{appendage_string}.', '', cleaned_company_name, flags=re.IGNORECASE)
        if f" {appendage_string}" in cleaned_company_name.lower():
            cleaned_company_name = re.sub(rf'\s*{appendage_string}', '', cleaned_company_name, flags=re.IGNORECASE)
        return cleaned_company_name
    cleaned_company_name = clean_appendage_strings('llc', cleaned_company_name=cleaned_company_name)
    cleaned_company_name = clean_appendage_strings('ltd', cleaned_company_name=cleaned_company_name)
    cleaned_company_name = clean_appendage_strings('inc', cleaned_company_name=cleaned_company_name)

    if cleaned_company_name.isupper() == True:
        words = cleaned_company_name.split()
        camel_case_words = [word.capitalize() for word in words]
        cleaned_company_name = ' '.join(camel_case_words)
    
    emoji_pattern = re.compile("\s*["
                               u"\U0001F600-\U0001F64F"  # Emoticons
                               u"\U0001F300-\U0001F5FF"  # Symbols & Pictographs
                               u"\U0001F680-\U0001F6FF"  # Transport & Map Symbols
                               u"\U0001F700-\U0001F77F"  # Alphanumeric Supplement
                               u"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                               u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                               u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                               u"\U0001FA00-\U0001FA6F"  # Chess Symbols
                               u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                               u"\U00002702-\U000027B0"  # Dingbats
                               u"\U000024C2-\U0001F251"
                               u"\u00A9\u00AE" 
                               u"\u2122"
                               "]\s*", flags=re.UNICODE)
    cleaned_company_name = emoji_pattern.sub(r'', cleaned_company_name)

    return cleaned_company_name