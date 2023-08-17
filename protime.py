from urllib.parse import urlparse, parse_qs
import requests
import time
from multiprocessing import Pool

def fetch_data(url, headers):
    response = requests.get(url, headers=headers)
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    current_page = query_params.get("searchCriteria[currentPage]", [0])
    
    if response.status_code == 200:
        print(f"Processed page: {current_page}")
        return response.json()
    else:
        handle_error(response)

def handle_error(response):
    if response.status_code == 404:
        error_data = response.json()
        error_message = error_data.get("message")
        error_parameters = error_data.get("parameters", {})
        field_name = error_parameters.get("fieldName")
        field_value = error_parameters.get("fieldValue")
        print(f"404 Error: {error_message}, {field_name} = {field_value} record not found.")
    else:
        print("Error details:", response.text)

if __name__ == '__main__':
    start_time = time.time()

    magento_url = f'{{url}}'
    headers = {
        'Authorization': '{{Token_info}}'
    }

    response = requests.get(magento_url, headers=headers)
    
    if response.status_code == 200:
        data_count = response.json()
        total = int(data_count['total_count'])
        base_url = '{{base_url}}'
        page_size = 200
        total_process_qty = int((total/page_size))
        num_pages = total_process_qty + 1
        print(num_pages)
    else:
        print("Error")
    
    urls = [
        f'{base_url}&searchCriteria[page_size]={page_size}&searchCriteria[currentPage]={page}'
        for page in range(1, num_pages+1)
    ]

    with Pool(processes=8) as pool:
        results = pool.starmap(fetch_data, [(url, headers) for url in urls])
    
    #print(results)
    print("--- %s seconds ---" % (time.time() - start_time))
