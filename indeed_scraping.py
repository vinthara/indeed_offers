import numpy as np
import time
import json
import pandas as pd
import re
import requests
import datetime
import os
import psycopg2

from random import randrange
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

offer_age = 1

def scrap_offer_indeed(list_keyword, offer_age, indeed_country, headless=True):
    """Scrap indeed offers using chrome browser.

    Args:
        list_keyword: word that will be searched in indeed.
        offer_age: age of offer in days.
        indeed_country: 2-length string country to be used in url.
        headless: whether to open a chrome browser or not, default set to True.
    
    Returns:
        soup_list: list of html strings to be parsed.

    Example:
        This example will scrap offers from the last 3 days of web developper, data analyst in the United Kingdom. 

        scrap_offer_indeed(["web developper", "data analyst"], 3, "uk", False)

    """
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1024,768")
    chrome_options.add_argument("--no-sandbox")
    if headless:
        chrome_options.add_argument("--headless")
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')
    
    s = ChromeService(ChromeDriverManager().install())
                      
    driver = webdriver.Chrome(service=s, options=chrome_options)
    cookies_clicked_already = False
    soup_list = []
    
    for keyword in list_keyword:
        url_base = f"https://{indeed_country}.indeed.com"
        url_param = f"/jobs?q={keyword}&fromage={offer_age}"

        while url_param:
            url = url_base + url_param
            driver.get(url)

            html_text = driver.page_source
            soup = BeautifulSoup(html_text, 'html.parser')

            if not cookies_clicked_already:
                for button in soup.find_all("button"):
                    if "All Cookies" in button.text or "les cookies" in button.text:
                        break
                if button.attrs.get("id"):
                    button_class = button.attrs["id"]
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f'button[id="{button_class}"]'))).click()
                    seconds = 3 + randrange(5, 11)/10
                    time.sleep(seconds)
                cookies_clicked_already = True

            html_text = driver.page_source
            soup = BeautifulSoup(html_text, 'html.parser')

            param = soup.find("nav", {"role":"navigation"}).find("a", {"aria-label":"Next Page"}) 
            if param:
                url_param = param["href"]
            else:
                url_param = None
            soup_list.append(soup)
    driver.close()
    return soup_list

def parse_indeed_soup(soup_list, indeed_country):
    """Parse the indeed offers to readable pandas DataFrame.

    Args:
        soup_list: list of html strings to be parsed.
        indeed_country: 2-length string country to be used in url
    
    Returns:
        df_jobs: panda DataFrame with an indeed offer and overall information.
        db_tags: panda DataFrame with job tags such as contract type, salary and more.
    
    """
    id_list = []
    job_title_list = []
    company_name_list = []
    job_location_list = []
    description_snippet_list = []
    rating_list = []
    scraped_at_list = []
    url_list = []
    list_dict_tag = []

    for s in soup_list:
        soup_beacon = s.find_all("div", {"class":"job_seen_beacon"})
        for soup in soup_beacon:
            id_ = soup.a["data-jk"]

            job_title = soup.find("span", {"title":True})
            company_name = soup.find("span", {"class":"companyName"})
            job_location = soup.find("div", {"class":"companyLocation"})
            rating = soup.find("span", {"class":"ratingNumber"})
            scraped_at = datetime.datetime.now().replace(microsecond=0)
            url = f'https://{indeed_country}.indeed.com/viewjob?jk={id_}'
            
            id_list.append(id_)
            url_list.append(url)
            
            if job_title:
                job_title_list.append(job_title.text)
            else:
                job_title_list.append(None)

            if company_name:
                company_name_list.append(company_name.text)
            else:
                company_name_list.append(None)

            if job_location:
                job_location_list.append(job_location.text)
            else:
                job_location_list.append(None)

            if rating:
                rating_list.append(rating.text)
            else:
                rating_list.append(None)

            scraped_at_list.append(scraped_at)

            # Job tags
            soup_tag = soup.find_all("div", {"class":"attribute_snippet"})
            for ele in soup_tag:
                list_dict_tag.append(
                    {
                        "id_job":id_,
                        "job_tag":ele.text.replace('\xa0', ' ')
                    }
                )

            description_snippet = soup.find("div", {"class":"job-snippet"})


            if description_snippet:
                description_snippet_list.append(description_snippet.text.strip().replace(u'\xa0', u' '))
            else:
                description_snippet_list.append(None)

    dict_jobs = {
        "id":id_list,
        "job_title":job_title_list,
        "company_name":company_name_list,
        "job_location":job_location_list,
        "description_snippet":description_snippet_list,
        "company_rating":rating_list,
        "scraped_at":scraped_at_list,
        "url":url_list
    }

    df_jobs = pd.DataFrame(dict_jobs)
    df_jobs = df_jobs.drop_duplicates(subset='id')
    df_jobs["company_rating"] = df_jobs["company_rating"].str.replace(",",".").astype("float")
    
    df_tags = pd.DataFrame(list_dict_tag)
    df_tags = df_tags.drop_duplicates(subset=['id_job', 'job_tag'])
    
    return df_jobs, df_tags

def insert_and_update_table(df_jobs, df_tags):
    """Insert new rows and update existing rows to the jobs and tags table given a .json connection file.

    Args:
        df_jobs: panda DataFrame with an indeed offer and overall information.
        db_tags: panda DataFrame with job tags such as contract type, salary and more.
    
    """

    with open("postgresql_credentials.json") as f:
        creds = json.load(f)
    
    conn_string = f'postgresql://{creds["username"]}:{creds["password"]}@{creds["host"]}/{creds["database"]}'

    engine = create_engine(conn_string)
    
    with engine.begin() as conn_alchemy:
        df_jobs.to_sql('jobs_to_add', conn_alchemy, if_exists='replace')
        df_tags.to_sql('tags_to_add', conn_alchemy, if_exists='replace')
    sql_insert_and_update_jobs = """
    INSERT INTO jobs (id, job_title, company_name, description_snippet, company_rating, scraped_at, url)
    SELECT 
        new.id,
        new.job_title,
        new.company_name,
        new.description_snippet,
        new.company_rating,
        new.scraped_at,
        new.url
    FROM jobs_to_add AS new
    LEFT JOIN jobs ON jobs.id = new.id
    WHERE jobs.id IS NULL;

    WITH jobs_to_update AS (
        SELECT
            new.id,
            new.job_title,
            new.company_name,
            new.description_snippet,
            new.company_rating,
            new.scraped_at
        FROM jobs_to_add AS new
        INNER JOIN jobs ON jobs.id = new.id
    ) 
    UPDATE jobs
    SET 
        job_title = jobs_to_update.job_title,
        company_name = jobs_to_update.company_name,
        description_snippet = jobs_to_update.description_snippet,
        company_rating = jobs_to_update.company_rating
    FROM jobs_to_update
    WHERE jobs.id = jobs_to_update.id;

    DROP TABLE jobs_to_add;
    """
    sql_insert_tags = """
    INSERT INTO tags (id_job, job_tag)
    SELECT 
        new.id_job,
        new.job_tag
    FROM tags_to_add AS new
    LEFT JOIN tags ON tags.id_job = new.id_job
    WHERE tags.id_job IS NULL;

    DROP TABLE tags_to_add;
    """
    conn = psycopg2.connect(
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        host=creds["host"]
    )


    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_insert_and_update_jobs)
            cur.execute(sql_insert_tags)

def get_id_and_url_indeed_description_to_scrap(n_offers):
    """Scrap latest full description of indeed offers.

    Args:
        n_offers: number of description to be scraped.

    Returns:
        id_list: list of id of the indeed offers.
        url_list: list of url of the indeed offers.

    Example:
        Get the 10 latest id and url of the indeed offers.

        get_id_and_url_indeed_description_to_scrap(1O)
    
    """
    with open("postgresql_credentials.json") as f:
        creds = json.load(f)
        
    conn_string = f'postgresql://{creds["username"]}:{creds["password"]}@{creds["host"]}/{creds["database"]}'

    engine = create_engine(conn_string)
    sql_query = f"""
    SELECT id, url FROM jobs
    WHERE description_verbose IS NULL
    ORDER BY updated_at DESC
    LIMIT {n_offers}
    """
    
    with engine.begin() as conn_alchemy:
        df = pd.read_sql(sql_query, conn_alchemy)

    id_list = df["id"].to_list()
    url_list = df["url"].to_list()
    
    return id_list, url_list

def scrap_indeed_description(id_offers_to_scrap, url_offers_to_scrap, headless=True):
    """Scrap indeed description using chrome browser.

    Args:
        id_offers_to_scrap: word that will be searched in indeed.
        url_offers_to_scrap: age of offer in days.
        headless: whether to open a chrome browser or not, default set to True.
    
    Returns:
        df_description: pandas DataFrame id, url and the verbose description of an offer.

    Example:
        This example will scrap the full description of those 2 offers id, note the fr country in the url. 

        scrap_indeed_description(
            ['24beaf6498294e77', '60c4fa9e88ed2de9'], 
            ['https://fr.indeed.com/viewjob?jk=24beaf6498294e77','https://fr.indeed.com/viewjob?jk=60c4fa9e88ed2de9'], 
            False)

    """
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1024,768")
    chrome_options.add_argument("--no-sandbox")
    if headless:
        chrome_options.add_argument("--headless")
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')

    s = ChromeService(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=s, options=chrome_options)
    
    description_verbose_list = []
    
    cookies_clicked_already = False
    for url in url_offers_to_scrap:     
        driver.get(url)
        html_text = driver.page_source
        
        soup = BeautifulSoup(html_text, 'html.parser')   
        

        if not cookies_clicked_already:
            button = None
            
            for button in soup.find_all("button"):
                if "All Cookies" in button.text or "les cookies" in button.text:
                    break
            if button:
                if button.attrs.get("id"):
                    button_class = button.attrs["id"]
                    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f'button[id="{button_class}"]'))).click()
                    seconds = 3 + randrange(5, 11)/10
                    time.sleep(seconds)
            cookies_clicked_already = True

        soup_description = soup.find_all("div",{"id":"jobDescriptionText"})

        for ele in soup_description:
            if ele:
                description_verbose_list.append(ele.text.replace(u'\xa0', u' '))
        if not ele:
            description_verbose_list.append(None)
        seconds = 3 + randrange(5, 11)/10
        time.sleep(seconds)
    driver.close()
        
    dict_description = {
        "id":id_offers_to_scrap,
        "url":url_offers_to_scrap,
        "description_verbose":description_verbose_list
    }

    df_description = pd.DataFrame(dict_description)
    return df_description

def update_indeed_description_verbose(df_description):
    """Update description_verbose rows in the jobs table given a .json connection file.

    Args:
        df_jobs: panda DataFrame with an indeed offer and overall information.
        db_tags: panda DataFrame with job tags such as contract type, salary and more.
    
    """
    with open("postgresql_credentials.json") as f:
        creds = json.load(f)
    
    conn_string = f'postgresql://{creds["username"]}:{creds["password"]}@{creds["host"]}/{creds["database"]}'

    engine = create_engine(conn_string)
    
    with engine.begin() as conn_alchemy:
        df_description.to_sql('description_verbose_to_update', conn_alchemy, if_exists='replace')
        
    conn = psycopg2.connect(
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        host=creds["host"]
    )


    with conn:
        with conn.cursor() as cur:
            sql_update_description_verbose = """
            UPDATE jobs
                SET description_verbose = d.description_verbose
            FROM description_verbose_to_update AS d
            WHERE jobs.id = d.id;
            
            DROP TABLE description_verbose_to_update;
            """
            cur.execute(sql_update_description_verbose)

if __name__ == '__main__':
    offer_age = 1
    list_country = ['fr', 'ca', 'uk', 'au']

    for indeed_country in list_country:
        if indeed_country == 'fr':
            list_keyword = ['data', 'développeur web', 'designer UX/UI']
        else:
            list_keyword = ['data', 'web developper', 'UX/UI designer']
        soup_list = scrap_offer_indeed(list_keyword, offer_age, indeed_country, False)
        df_jobs, df_tags = parse_indeed_soup(soup_list, indeed_country)
        insert_and_update_table(df_jobs, df_tags)

    for i in range(130):
        id_offers_to_scrap, url_offers_to_scrap = get_id_and_url_indeed_description_to_scrap(15)
        if id_offers_to_scrap:
            df_description = scrap_indeed_description(id_offers_to_scrap, url_offers_to_scrap, False)
            update_indeed_description_verbose(df_description)
        else:
            break