from flask import Flask, jsonify, request, make_response
import requests
import os
from flask_cors import CORS, cross_origin
from dotenv import load_dotenv
from redis import Redis
from apscheduler.schedulers.background import BackgroundScheduler
import json
from pytz import utc
import gzip
from collections import Counter
from datetime import datetime
from collections import defaultdict
from datetime import datetime, timedelta
import base64
load_dotenv()

app = Flask(__name__)
base_url = os.environ.get("BASE_URL", "http://localhost:3000")

allowed_origins = [
    base_url, 
    "https://www.insiderhood.com", 
    "https://insiderhood.com"
]

CORS(app, resources={
    r"/311calls": {"origins": allowed_origins},
    r"/311calls_complaint_types_count": {"origins": allowed_origins},
    r"/dob_approved_permits": {"origins": allowed_origins}
}, supports_credentials=True)


# Use environment variables for Redis host and port
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis = Redis(host=redis_host, port=redis_port, db=0, decode_responses=False)  

scheduler = BackgroundScheduler(timezone=utc)
scheduler.start()

def compress_data(data):
    # Ensure data is a JSON string and then compress
    if not isinstance(data, bytes):
        data = json.dumps(data).encode('utf-8')
    compressed_data = gzip.compress(data)
    # Encode compressed data in base64 to store as string in Redis
    base64_encoded_data = base64.b64encode(compressed_data).decode('utf-8')
    return base64_encoded_data


def decompress_data(data):
  
    try:
        # Decode the data from base64
        base64_decoded_data = base64.b64decode(data)
        # Attempt to decompress and decode
        decompressed_data = gzip.decompress(base64_decoded_data).decode('utf-8')
        return json.loads(decompressed_data)
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as e:
        # Fallback to plain JSON loading if data is not compressed or base64 encoded
        print("Error during decompression or decoding:", e)
        try:
            # This assumes the data could be plain JSON text
            return json.loads(data)
        except json.JSONDecodeError as json_error:
            print("Final fallback, unable to decode JSON:", json_error)
            return None


# Background task to fetch data and cache in Redis
def fetch_and_cache_data(data_source):

    if data_source == '311calls':
        response = requests.get('https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=50000')
        if response.status_code == 200:
            raw_data = response.json()
            filtered_data = [
                {
                    'Created Date': item.get('created_date', 'Not specified'),
                    'Agency': item.get('agency', 'Not specified'),
                    'Agency Name': item.get('agency_name', 'Not specified'),
                    'Complaint Type': item.get('complaint_type', 'Not specified'),
                    'Descriptor': item.get('descriptor', 'Not specified'),
                    'Location Type': item.get('location_type', 'Not specified'),
                    'Incident Zip': item.get('incident_zip', 'Not specified'),
                    'Incident Address': item.get('incident_address', 'Not specified'),
                    'Borough': item.get('borough', 'Not specified'),
                    'Location': item.get('location', {'latitude': 'Not specified', 'longitude': 'Not specified'})
                } for item in raw_data
            ]

            dates = [datetime.strptime(item["Created Date"], "%Y-%m-%dT%H:%M:%S.%f") for item in filtered_data]
            min_date = min(dates)
            max_date = max(dates)

            min_date_str = min_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
            max_date_str = max_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
            redis.hset("complaints_date_range", "min_date", min_date_str)
            redis.hset("complaints_date_range", "max_date", max_date_str)
            redis.expire("complaints_date_range", 86400)  # Set expiration to 24 hours

            compressed_data = compress_data(filtered_data)

            redis.setex('complaints_data', 86400, compressed_data)
            print("Data fetched and compressed and cached")
            return filtered_data
        else:
            print("Failed to fetch data from API")
            return []
    elif data_source == "dob_approved_permits":
        # make the request to the dob approved permits:
        response = requests.get('https://data.cityofnewyork.us/resource/rbx6-tga4.json?$limit=50000')
        if response.status_code == 200:
            raw_data = response.json()
            filtered_data = [
                {
                    'House Number': item.get('house_no', 'Not specified'),
                    'Street Name': item.get('street_name', 'Not specified'),
                    'Borough': item.get('borough', 'Not specified'),
                    'Community Board': item.get('c_b_no', 'Not specified'),
                    'Work Floor': item.get('work_on_floor', 'Not specified'),
                    'Work Type': item.get('work_type', 'Not specified'),
                    'Applicant Business Name': item.get('applicant_business_name', 'Not specified'),
                    'Approved Date': item.get('approved_date', 'Not specified'),
                    'Issued Date': item.get('issued_date', 'Not specified'),
                    'Expired Date': item.get('expired_date', 'Not specified'),
                    'Job Description': item.get('job_description', 'Not specified'),
                    'Estimated Cost': item.get('estimated_job_costs', 'Not specified'),
                    'Owner Business Name': item.get('owner_business_name', 'Not specified')
                } for item in raw_data
            ]

            compressed_data = compress_data(filtered_data)
            redis.setex('dob_approved_permits', 86400, compressed_data)
            print("Data fetched and compressed and cached")
            return filtered_data
        else:
            print("Failed to fetch data from API")
            return []

# Schedule the fetch_and_cache_data to run daily at 4:00 AM
scheduler.add_job(fetch_and_cache_data, 'cron', args=['311calls'], hour=4)


@app.route('/311calls', methods=['GET'])
@cross_origin(origin='*', supports_credentials=True)
def calls311():
    filters = {
        # 'Incident Zip': request.args.get('IncidentZip', '').strip(),
        # 'Borough': request.args.get('Borough', '').strip(),
        'Agency': request.args.get('Agency', '').strip(), 
        'Created Date': request.args.get('CreatedDate', '').strip()
    }
    boroughs = request.args.getlist('Borough[]')
    zip_codes = request.args.getlist('zip[]')
    complaint_type = request.args.get('ComplaintType')
    initialLoad= request.args.get("initialLoad")

    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 0))
    cached_data = redis.get('complaints_data') 
    date_range = redis.hgetall('complaints_date_range')


    if cached_data and date_range:
            
        data = decompress_data(cached_data)
        min_date = date_range.get(b'min_date').decode('utf-8')  # Ensure to handle byte keys if necessary
        max_date = date_range.get(b'max_date').decode('utf-8')

        if boroughs:
            filters['Borough'] = [borough.upper() for borough in boroughs] 

        if zip_codes:
            filters['Incident Zip'] = zip_codes

        if complaint_type:
            filters['Complaint Type'] = complaint_type
        
        # Filter data with updated filters, including zip codes if provided
        filtered_data = filter_data(data, filters)

        # descriptor counts:
        if zip_codes:
            # Count by Complaint Type and Zip Code
            descriptor_counts = defaultdict(lambda: defaultdict(int))
            for item in filtered_data:
                complaint_type = item['Complaint Type'].title()
                item_zip = item.get('Incident Zip', 'Unknown')
                descriptor_counts[complaint_type][item_zip] += 1
            
            # Convert dictionary to list format
            descriptor_counts = [
                {"name": complaint, **dict(zip_counts)} for complaint, zip_counts in descriptor_counts.items()
            ]
        else:
            # Original counting method when no zip codes are provided
            descriptor_counts = Counter(item['Complaint Type'].title() for item in filtered_data)

        # Calculate counts of descriptors and times for the filtered data
        # descriptor_counts = Counter(item['Complaint Type'].title() for item in filtered_data)

        hour_minute_counts = count_hour_minute(filtered_data)
        data_count_by_day = [] 


        # this will only take place in the first request:
        if initialLoad:

            aggregated_data = defaultdict(lambda: defaultdict(int))
            # Formatting the output
            data_count_by_day = []  # Ensure data_count_by_day is define

            # Check if zip_codes is provided and filter items based on 'Incident Zip'
            if zip_codes:
                # Only process if zip_codes are provided
                for item in filtered_data:
                    zip_code = item['Incident Zip']
                    if zip_code in zip_codes:
                        date_only = item['Created Date'][:10]  # Extract only the date part (YYYY-MM-DD)
                        aggregated_data[date_only][zip_code] += 1

                # Reformat the output to be by date, with zip code counts
                for date, zips in aggregated_data.items():
                    response_entry = {"date": date}
                    for zip_code, count in zips.items():
                        response_entry[zip_code] = count
                    data_count_by_day.append(response_entry)
            else:
                for item in filtered_data:
                    borough = item['Borough']
                    date_only = item['Created Date'][:10]  # Extract only the date part (YYYY-MM-DD)
                    aggregated_data[borough][date_only] += 1

                for borough, dates in aggregated_data.items():
                    response_entry = {"Borough": borough}
                    for date, count in dates.items():
                        response_entry[date] = count
                    data_count_by_day.append(response_entry)



        # Check if a valid limit is provided
        if limit > 0:
            start = (page - 1) * limit
            end = start + limit
            paginated_data = filtered_data[start:end]
            response = paginated_data

        else:
                # If limit is 0 or not provided, return all filtered data
            response = filtered_data

        response_data = {
                "original_data": response,
                "hour_minute_counts": hour_minute_counts,
                "descriptor_counts":descriptor_counts,
                "min_date": min_date,  # Add min date to response
                "max_date": max_date, 
                "data_length":len(filtered_data),
                "data_count_by_day":data_count_by_day
        }
        return jsonify(response_data)
    else:
        # If no cached data, fetch and cache the data
        fetch_and_cache_data("311calls")


@app.route('/311calls_complaint_types_count', methods=['GET'])
@cross_origin(origin='*', supports_credentials=True)
def complaint_types_count():

    boroughs = request.args.getlist('boroughs[]')
    zip_codes = request.args.getlist('zipcodes[]')
    complaint_types = request.args.getlist('complaint_types[]')

    # Redis: Retrieve the date range
    date_range = redis.hgetall('complaints_date_range')

    start_date = date_range.get(b'min_date').decode('utf-8')  # Ensure to handle byte keys if necessary
    end_date = date_range.get(b'max_date').decode('utf-8')
    # start_date = date_range['min_date'].decode('utf-8')
    # end_date = date_range['max_date'].decode('utf-8')

    filters={}
    if boroughs:
        filter_type = 'Borough'
        filter_keys = [borough.upper() for borough in boroughs]

    if zip_codes:
        filter_type = 'Incident Zip'
        filter_keys = zip_codes

    cached_data = redis.get('complaints_data') 
    data = []
    if cached_data:
        data = decompress_data(cached_data)

    chart_ready_data = format_data_for_chart(data, start_date, end_date, filter_type, filter_keys, complaint_types)

    return jsonify(chart_ready_data)
    
    
@app.route('/dob_approved_permits', methods=['GET'])
@cross_origin(origin='*', supports_credentials=True)
def dob_approved_permits():
    filters ={
        # 'Incident Zip': request.args.get('IncidentZip', '').strip(),
        # 'Borough': request.args.get('Borough', '').strip(),
        # 'Agency': request.args.get('Agency', '').strip(), 
        # 'Created Date': request.args.get('CreatedDate', '').strip()
    }

    cached_data = redis.get("dob_approved_permits")
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))

    if cached_data:
        data = decompress_data(cached_data)
        filtered_data = filter_data(data, filters)
         # Check if a valid limit is provided
        if limit > 0:
            start = (page - 1) * limit
            end = start + limit
            paginated_data = filtered_data[start:end]
            response = make_response(jsonify(paginated_data))
        else:
                 # If limit is 0 or not provided, return all filtered data
            response = make_response(jsonify(filtered_data))        
        return response 
    else:
        fetch_and_cache_data("dob_approved_permits")

        
def filter_data(data, filters):
    filtered_data = [
        item for item in data
        if all(
            (item[key] == value if isinstance(value, str) else item[key] in value)
            or value == '' 
            for key, value in filters.items()
        )
    ]
    return filtered_data

def format_data_for_chart(data, start_date, end_date, filter_type, filter_keys, complaint_types):
    current_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S.%f")
    end_date = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S.%f")
    date_format = "%Y-%m-%d"

    # Create a list to hold the data in the required format for the chart
    chart_data = []

    # Initialize data structure for each day
    while current_date <= end_date:
        formatted_date = current_date.strftime(date_format)
        day_data = {'date': formatted_date}
        for key in filter_keys:
            day_data[key] = 0  # Initialize each filter key count as 0 for this day
        chart_data.append(day_data)
        current_date += timedelta(days=1)

    # Populate the data
    for entry in data:
        entry_date = datetime.strptime(entry['Created Date'], "%Y-%m-%dT%H:%M:%S.%f").strftime(date_format)
        if entry[filter_type] in filter_keys and (not complaint_types or entry['Complaint Type'] in complaint_types):
            for day_data in chart_data:
                if day_data['date'] == entry_date:
                    day_data[entry[filter_type]] += 1

    return chart_data


def count_hour_minute(data):
    # Extract 'hour:minute' and count occurrences
    hour_minute_list = [
        datetime.strptime(item['Created Date'], '%Y-%m-%dT%H:%M:%S.%f').strftime('%H:%M')
        for item in data if 'Created Date' in item
    ]
    return dict(Counter(hour_minute_list))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
