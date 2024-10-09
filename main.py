import re
import requests
import json
import sys
from sqlite3 import Error
from bs4 import BeautifulSoup
import time as tm
from itertools import groupby
from datetime import datetime, timedelta, time
import pandas as pd
from urllib.parse import quote


def load_config(file_name):
    # Load the config file
    with open(file_name) as f:
        return json.load(f)


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


def main(config_file):
    start_time = tm.perf_counter()
    job_list = []

    config = load_config(config_file)

    all_jobs = get_jobcards(config)
    # conn = create_connection(config)

    # filtering out jobs that are already in the database
    print("Total new jobs found after comparing to the database: ", len(all_jobs))

    if len(all_jobs) > 0:

        for job in all_jobs:
            job_date = convert_date_format(job["date"])
            job_date = datetime.combine(job_date, time())

            # if job is older than a week, skip it
            if job_date < datetime.now() - timedelta(days=config["days_to_scrape"]):
                continue
            print(
                "Found new job: ", job["title"], "at ", job["company"], job["job_url"]
            )
            desc_soup = get_with_retry(job["job_url"], config)
            extracted_data = transform_job(desc_soup)
            job["job_description"] = extracted_data["text"]
            job["email"] = extracted_data["email_address"]
            job["telephone"] = extracted_data["telephone_number"]
            job_list.append(job)

        print("Total jobs to add: ", len(job_list))

        print(job_list[0])

    else:
        print("No jobs found")

    end_time = tm.perf_counter()
    print(f"Scraping finished in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    config_file = "config.json"  # default config file

    main(config_file)
