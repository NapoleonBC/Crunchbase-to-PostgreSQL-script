import urllib.parse
import re
from datetime import datetime

def name_validator(name):
    # Replace hyphens with underscores in the name
    name = name.replace('-', '_')
    return name

def convert_to_valid_date_format(s):
    try:
        # Attempt to parse the input as a Unix timestamp
        timestamp = int(s)
        # Convert the timestamp to a datetime object
        date_obj = datetime.fromtimestamp(timestamp)
        # Convert the datetime object to the desired format "YYYY-MM-DD"
        formatted_date = date_obj.strftime("%Y-%m-%d")
        return formatted_date
    except Exception as e:
        # If the input is not a valid timestamp, return the input as it is (assuming it's already in valid format)
        return s

def remove_unnecessary_characters(text):
    # Use regular expression to find the URL enclosed in angle brackets
    try:
        pattern = r"<(.*?)>"
        match = re.search(pattern, text)
        
        if match:
            # Get the matched URL
            url = match.group(1)
            return f"<{url}>"
    except Exception as e:
        print (e.args[0])
    
    return None

def extract_original_url(text):
    # Find the position of "_:x" in the text
    while True:
        start_idx = text.find("_:")

        # If "_:x" is found, find the position of the first space after "_:x"
        if start_idx != -1:
            try:
                end_idx = text.find(" ", start_idx)

                # Extract the URL substring between "_:x" and the first space
                url_encoded = text[start_idx + 2:end_idx]
                
                # Replace the encoded characters and remove "_:x" occurrences
                decoded_text = url_encoded.replace('_:x', '').replace('x3A', ':').replace('x2E', '.').replace('x2F', '/').replace('x2D', '-').replace('x3C', '<').replace('x3E', '>')
                
                # Use urllib.parse.unquote to decode the URL
                decoded_url = urllib.parse.unquote(decoded_text)
                decoded_url = remove_unnecessary_characters(decoded_url)
                text = text[:start_idx] + (decoded_url if decoded_url else "") + text[end_idx:]
            except Exception as e:
                print (e.args[0])
                return text
        else:
            # If no "_:x" pattern is found, return the original text
            result = text
            return result
    
def extract_table_name_and_id(url):
    # Find the position of "api" in the URL
    try:
        api_idx = url.find("api")

        if api_idx != -1:
            # Get the substring after "api/"
            url_after_api = url[api_idx + len("api/"):]

            # Split the remaining URL by slashes '/'
            parts = url_after_api.strip().split('/')
            if len(parts) >= 2:
                first_word = parts[0]
                second_word = parts[1]
                if "#" in second_word:
                    second_word = second_word.split("#")[0]

                return first_word, second_word
    except Exception as e:
        print (e.args[0])
    # print("Invalid URL.")
    return None, None

def extract_field_name(url):
    try:
        sharp_idx = url.find("#")

        if sharp_idx != -1:
            field_name = url[sharp_idx+1:]
            if field_name == "id":
                field_name = url[:sharp_idx].split('/')
                if len(field_name)>0:
                    field_name = field_name[-1]
            return field_name
    except Exception as e:
        print (e.args[0])
    return url

if __name__ == '__main__':
    # Sample input
    text = "_:x5Fx3Ab0xxx3Chttpx3Ax2Fx2Fkmx2Eaifbx2Ekitx2Eedux2Fservicesx2Fcrunchbasex2Fapix2Fpeoplex2Fidrisx2Dsamix3E <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ontologycentral.com/2010/05/cb/vocab#Organization>"
    text1 = "_:x5Fx3Ab0xxx3Chttpx3Ax2Fx2Fkmx2Eaifbx2Ekitx2Eedux2Fservicesx2Fcrunchbasex2Fapix2Fpeoplex2Fidrisx2Dsamix3E <http://ontologycentral.com/2010/05/cb/vocab#acquiree> _:x5Fx3Ab0xxx3Chttpx3Ax2Fx2Fkmx2Eaifbx2Ekitx2Eedux2Fservicesx2Fcrunchbasex2Fapix2Facquisitionsx2F00025ebe489ec4e4dcaec3e8941bc140x3E ."
    text2 = "<http://km.aifb.kit.edu/services/crunchbase/api/acquisitions/00025ebe489ec4e4dcaec3e8941bc140#id> <http://km.aifb.kit.edu/services/crunchbase/api-vocab#news> <http://km.aifb.kit.edu/services/crunchbase/api/acquisitions/00025ebe489ec4e4dcaec3e8941bc140/news> .\n"
    text3 = '<http://km.aifb.kit.edu/services/crunchbase/api/acquisitions/00025ebe489ec4e4dcaec3e8941bc140#id>'
    original_url = extract_original_url(text3)
    print(original_url)
    print (extract_table_name_and_id(original_url))
    print (extract_field_name('<http://ontologycentral.com/2010/05/cb/vocab#Organization>'))