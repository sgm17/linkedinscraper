import os
import re
from typing import List
import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import time as tm
from itertools import groupby
from datetime import datetime, timedelta, time
from urllib.parse import quote
from notion_client import Client

from job_offer import JobOffer

# Load environment variables
load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_API_KEY")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")
notion = Client(auth=NOTION_TOKEN)


def load_config():
    # Load the config file
    with open("config.json") as f:
        return json.load(f)


config = load_config()


def get_with_retry(url, config, retries=3, delay=1):
    # Get the URL with retries and delay
    for i in range(retries):
        try:
            if len(config["proxies"]) > 0:
                r = requests.get(
                    url, headers=config["headers"], proxies=config["proxies"], timeout=5
                )
            else:
                r = requests.get(url, headers=config["headers"], timeout=5)
            return BeautifulSoup(r.content, "html.parser")
        except requests.exceptions.Timeout:
            print(f"Timeout occurred for URL: {url}, retrying in {delay}s...")
            tm.sleep(delay)
        except Exception as e:
            print(f"An error occurred while retrieving the URL: {url}, error: {e}")
    return None


def transform(soup):
    # Parsing the job card info (title, company, location, date, job_url) from the beautiful soup object
    joblist = []
    try:
        divs = soup.find_all("div", class_="base-search-card__info")
    except:
        print("Empty page, no jobs found")
        return joblist
    for item in divs:
        title = item.find("h3").text.strip()
        company = item.find("a", class_="hidden-nested-link")
        location = item.find("span", class_="job-search-card__location")
        parent_div = item.parent
        entity_urn = parent_div["data-entity-urn"]
        job_posting_id = entity_urn.split(":")[-1]
        job_url = "https://www.linkedin.com/jobs/view/" + job_posting_id + "/"

        date_tag_new = item.find("time", class_="job-search-card__listdate--new")
        date_tag = item.find("time", class_="job-search-card__listdate")
        date = (
            date_tag["datetime"]
            if date_tag
            else date_tag_new["datetime"] if date_tag_new else ""
        )
        job_description = ""
        job = {
            "title": title,
            "company": company.text.strip().replace("\n", " ") if company else "",
            "location": location.text.strip() if location else "",
            "date": date,
            "job_url": job_url,
            "job_description": job_description,
        }
        joblist.append(job)
    return joblist


def transform_job(soup):
    div = soup.find("div", class_="description__text description__text--rich")
    if div:
        # Remove unwanted elements
        for element in div.find_all(["span", "a"]):
            element.decompose()

        # Replace bullet points
        for ul in div.find_all("ul"):
            for li in ul.find_all("li"):
                li.insert(0, "-")

        text = div.get_text(separator="\n").strip()
        text = text.replace("\n\n", "")
        text = text.replace("::marker", "-")
        text = text.replace("-\n", "- ")
        text = text.replace("Show less", "").replace("Show more", "")

        # Extract telephone number using RegEx
        phone_regex = r"(\+?\d{1,3}[ -]?)?(\(0\)\s*|\(0\)\-)?(0|\d{2})\d{7}"
        telephone_numbers = re.findall(phone_regex, text)

        # Extract email address using RegEx
        email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        email_addresses = re.findall(email_regex, text)

        # Format the results
        phone_numbers = [
            num[0] or num[1] + num[2] for num in telephone_numbers
        ]  # Combine the parts
        emails = list(set(email_addresses))  # Remove duplicates

        return {
            "text": text,
            "email_address": emails[0] if email_addresses else None,
            "telephone_number": phone_numbers[0] if phone_numbers else None,
        }
    else:
        return {"text": None, "email_address": None, "telephone_number": None}


def remove_duplicates(joblist, config):
    # Remove duplicate jobs in the joblist. Duplicate is defined as having the same title and company.
    joblist.sort(key=lambda x: (x["title"], x["company"]))
    joblist = [
        next(g) for k, g in groupby(joblist, key=lambda x: (x["title"], x["company"]))
    ]
    return joblist


def convert_date_format(date_string):
    """
    Converts a date string to a date object.

    Args:
        date_string (str): The date in string format.

    Returns:
        date: The converted date object, or None if conversion failed.
    """
    date_format = "%Y-%m-%d"
    try:
        job_date = datetime.strptime(date_string, date_format).date()
        return job_date
    except ValueError:
        print(f"Error: The date for job {date_string} - is not in the correct format.")
        return None


def get_jobcards(config):
    # Function to get the job cards from the search results page
    all_jobs = []
    for k in range(0, config["rounds"]):
        for query in config["search_queries"]:
            keywords = quote(query["keywords"])  # URL encode the keywords
            location = quote(query["location"])  # URL encode the location
            for i in range(0, config["pages_to_scrape"]):
                url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&geoId=&f_TPR={config['timespan']}&start={25*i}"
                soup = get_with_retry(url, config)
                jobs = transform(soup)
                all_jobs = all_jobs + jobs
                print("Finished scraping page: ", url)
    print("Total job cards scraped: ", len(all_jobs))
    all_jobs = remove_duplicates(all_jobs, config)
    print("Total job cards after removing duplicates: ", len(all_jobs))
    return all_jobs


def retrieve_job_offers() -> List[JobOffer]:
    job_list = []

    all_jobs = get_jobcards(config)

    # filtering out jobs that are already in the database
    print("Total new jobs found after comparing to the database: ", len(all_jobs))

    if len(all_jobs) > 0:

        for job in all_jobs:
            job_date = convert_date_format(job["date"])
            job_date = datetime.combine(job_date, time())

            # if job is older than a week, skip it
            if job_date < datetime.now() - timedelta(days=config["days_to_scrape"]):
                continue

            desc_soup = get_with_retry(job["job_url"], config)
            extracted_data = transform_job(desc_soup)
            job["email"] = extracted_data["email_address"]
            job["telephone"] = extracted_data["telephone_number"]
            job["job_description"] = extracted_data["text"]

            # Check if job_description is not None before checking its length
            if (
                job["job_description"] is not None
                and len(job["job_description"]) > 2000
            ):
                # Reduce the length of the job description if it's too large
                job["job_description"] = job["job_description"][:2000]

            new_job = JobOffer(
                job["company"],
                job["title"],
                job["location"],
                job["date"],
                job["telephone"],
                job["job_description"],
                job["email"],
                job["job_url"],
            )
            job_list.append(
                new_job,
            )
            print(
                "Found new job: ", job["title"], "at ", job["company"], job["job_url"]
            )
        print("Total jobs to add: ", len(job_list))
        return job_list
    else:
        return job_list


def update_env_file() -> str:
    # Create notion database
    id = create_notion_database()

    with open(".env", "a") as env_file:
        env_file.write(f"\n{keyword}={id}")

    # Set the new variable
    return id


def retrieve_stored_jobs_from_notion() -> List[JobOffer]:
    response = notion.databases.query(database_id)

    jobs = []

    if not response:
        return jobs

    for page in response["results"]:
        properties = page["properties"]

        # Extract fields from the properties
        title = (
            properties["Title"]["title"][0]["text"]["content"]
            if properties["Title"]["title"]
            else None
        )
        company = (
            properties["Company"]["rich_text"][0]["text"]["content"]
            if properties["Company"]["rich_text"]
            else None
        )
        location = (
            properties["Location"]["rich_text"][0]["text"]["content"]
            if properties["Location"]["rich_text"]
            else None
        )
        timestamp = (
            properties["Timestamp"]["date"]["start"]
            if properties.get("Timestamp") and properties["Timestamp"]["date"]
            else None
        )
        description = (
            properties["Description"]["rich_text"][0]["text"]["content"]
            if properties["Description"]["rich_text"]
            else None
        )
        telephone = (
            properties["Telephone"]["phone_number"]
            if properties.get("Telephone")
            else None
        )
        email = properties["Email"]["email"] if properties.get("Email") else None
        url = properties["URL"]["url"] if properties.get("URL") else None

        new_job = JobOffer(
            company,
            title,
            location,
            timestamp,
            telephone,
            description,
            email,
            url,
        )
        jobs.append(new_job)

    return jobs


def update_notion_database(jobs: List[JobOffer]):
    def append_job_to_database(job: JobOffer):
        parsed_date = datetime.strptime(job.timestamp, "%Y-%m-%d")

        # Construct the body of the request
        body = {
            "parent": {
                "type": "database_id",
                "database_id": database_id,
            },
            "properties": {
                "Title": {
                    "type": "title",
                    "title": [{"type": "text", "text": {"content": job.title}}],
                },
                "Company": {
                    "type": "rich_text",
                    "rich_text": [{"type": "text", "text": {"content": job.company}}],
                },
                "Location": {
                    "type": "rich_text",
                    "rich_text": [{"type": "text", "text": {"content": job.location}}],
                },
                "Timestamp": {
                    "type": "date",
                    "date": {
                        "start": parsed_date.isoformat() + "Z",
                        "end": parsed_date.isoformat() + "Z",
                    },
                },
                "Description": {
                    "type": "rich_text",
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": job.description if job.description else ""
                            },
                        }
                    ],
                },
                "Telephone": {
                    "type": "phone_number",
                    "phone_number": job.telephone if job.telephone else "000000000",
                },
                "Email": {
                    "type": "email",
                    "email": job.email if job.email else "example@gmail.com",
                },
                "URL": {"type": "url", "url": job.url},
                "State": {"select": {"name": "😤Not applied"}},
            },
        }

        # Send the request to update the database
        notion.pages.create(**body)

    for job in jobs:
        append_job_to_database(job)
        print("Stored new job: ", job.title, "at ", job.company, job.url)


def create_notion_database() -> str:
    title = " ".join(keyword.split("_")).title()
    result = notion.databases.create(
        parent={"page_id": NOTION_PAGE_ID},
        title=[{"type": "text", "text": {"content": title, "link": None}}],
        properties={
            "Title": {"title": {}},
            "Company": {"rich_text": {}},
            "Location": {"rich_text": {}},
            "Timestamp": {"date": {}},
            "Description": {"rich_text": {}},
            "Telephone": {"phone_number": {}},
            "Email": {"email": {}},
            "URL": {"url": {}},
            "State": {
                "select": {
                    "options": [
                        {"name": "😤Not applied", "color": "red"},
                        {"name": "🍀Applied", "color": "blue"},
                        {"name": "👻Ghosted", "color": "yellow"},
                        {"name": "📚Interview", "color": "green"},
                        {"name": "🙅Rejected", "color": "yellow"},
                    ]
                }
            },
        },
    )

    database_id = result["id"]
    return database_id


if __name__ == "__main__":
    start_time = tm.perf_counter()

    for search_queries in config["search_queries"]:
        keyword = search_queries["keywords"]

        keyword = "_".join(keyword.split(" ")).upper()

        # Get the id of the db for the given keyword
        database_id = os.getenv(keyword)

        if not database_id:
            id = update_env_file()
            # Update the .env file with the new keyword
            os.environ[keyword] = id

        # Retrieve all the jobs from the db id
        stored_jobs = retrieve_stored_jobs_from_notion()

        # Get all the scrapped jobs
        new_jobs = retrieve_job_offers()

        # Substract the duplicated job offers
        jobs_to_store = [job for job in new_jobs if job not in stored_jobs]
        print(f"This is the number of new scrapped job offers: {len(jobs_to_store)}")

        update_notion_database(jobs_to_store)

        end_time = tm.perf_counter()
        print(f"Scraping finished in {end_time - start_time:.2f} seconds")
