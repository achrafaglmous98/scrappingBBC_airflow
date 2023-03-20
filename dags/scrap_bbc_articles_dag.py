from bs4 import BeautifulSoup
import requests
import dateparser
import csv, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Achraf',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 11), # start on March 11th, 2023
    'end_date': datetime(2023, 3, 19), # end on March 16th, 2023
    'email': ['achraf.agl@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_bbc_articles',
    default_args=default_args,
    schedule_interval='0 22 * * *'
)

def scrape_bbc_articles():
    bbc_url = "https://www.bbc.co.uk/news"
    base_url = "https://www.bbc.co.uk"
    response = requests.get(bbc_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    category_urls = []
    category_labels = []
    for category_link in soup.select('.nw-c-nav__wide a'):
        category = category_link['href']
        category_label=category_link.getText().strip()
        if category.startswith('/news/'):
            category_urls.append(f'{base_url}{category}')
            category_labels.append(category_label)
    articles = []
    for category_url in category_urls:
        category_response = requests.get(category_url)
        category_soup = BeautifulSoup(category_response.content, 'html.parser')
        subcategory_links = category_soup.select('.nw-c-nav__wide-secondary a')
        if subcategory_links:
            for subcategory_link in subcategory_links:
                subcategory_url = f"{base_url}{subcategory_link['href']}"
                article_subcategory = subcategory_link.getText().strip()
                subcategory_response = requests.get(subcategory_url)
                subcategory_soup = BeautifulSoup(subcategory_response.content, 'html.parser')
                promo_article_headings = subcategory_soup.select('.gs-c-promo')
                get_articles(promo_article_headings, base_url, category_labels, category_urls, category_url, article_subcategory, articles)
                Latest_updates_article_headings = subcategory_soup.select('.gs-o-media__body')
                get_articles(Latest_updates_article_headings, base_url, category_labels, category_urls, category_url, article_subcategory, articles)
                 
        else:
            article_subcategory = None
            promo_article_headings = category_soup.select('.gs-c-promo')
            get_articles(promo_article_headings, base_url, category_labels, category_urls, category_url, article_subcategory, articles)
            Latest_updates_article_headings = category_soup.select('.gs-o-media__body')
            get_articles(Latest_updates_article_headings, base_url, category_labels, category_urls, category_url, article_subcategory, articles)
    return articles 

def get_articles(article_headings, base_url, category_labels, category_urls, category_url, article_subcategory, articles):
    if article_headings:
        for article_heading in article_headings:
            article_link = article_heading.find('a')
            if article_link:
                article_url = article_link['href']
                if article_url.startswith('/news/'):
                    article_response = requests.get(f'{base_url}{article_url}')
                    article_soup = BeautifulSoup(article_response.content, 'html.parser')
                    article_category = category_labels[category_urls.index(category_url)]
                    article_title = get_article_title(article_soup)
                    article_date = get_article_date(article_soup)
                    article_topics = get_article_topics(article_soup)
                    article_authors = get_article_authors(article_soup)
                    article_body = get_article_body(article_soup)
                    article_images = get_article_images(article_soup)
                    articles.append({'Title': article_title, 'Date': article_date, 'Category': article_category, 
                                     'Subcategory': article_subcategory, 'Topic': article_topics, 'Authors': article_authors,
                                     'Text': article_body, 'Images': article_images})
                    

def get_article_title(soup):
    article_title_element = soup.select_one('h1#main-heading')
    if article_title_element is not None: #Checks if the element is not a none 
        article_title = article_title_element.getText().strip()
        return article_title
    else:
        print('No title found.')
        return None

def get_article_date(soup):
    date_element = soup.select_one('time')
    if date_element is not None:
        date_str = date_element.getText().strip()
        # Parse the relative time string to datetime object
        date_obj = dateparser.parse(date_str, settings={'RELATIVE_BASE': datetime.now()})
        if date_obj is not None:
            # Get the timestamp of the datetime object
            timestamp = date_obj.timestamp()
            # Convert timestamp to datetime object
            datetime_obj = datetime.fromtimestamp(timestamp)
            # Convert datetime object to string in format 'YYYY-MM-DD'
            date_string = datetime_obj.strftime('%Y-%m-%d')
            return date_string
        else:
            print('Failed to parse date string.')
            return None
    else:
        print('No date found.')
        return None
    
def get_article_body(soup):
    body_elements = soup.select('div[data-component="text-block"]')
    if body_elements:
        # Combine the text content of all the body elements
        article_body = ' '.join([elem.getText().strip() for elem in body_elements])
        return article_body
    else:
        print('No body found.')
        return None
    
def get_article_authors(soup):
    author_elements = soup.select('div[data-component="byline-block"]')
    if author_elements:
        authors = [elem.getText().strip() for elem in author_elements]
        return authors
    else:
        print('No authors found.')
        return None
    
def get_article_images(soup):
    image_elements = soup.select('div[data-component="image-block"] picture img')
    if image_elements:
        image_links = [elem.get('src') for elem in image_elements]
        return image_links
    else:
        print('No images found.')
        return None

def get_article_videos(soup):
    video_elements = soup.select('div[data-component="video-block"] video ')
    if video_elements:
        videos = [elem.get('src') for elem in video_elements]
        return videos
    else:
        print('No videos found.')
        return None

def get_article_topics(soup):
    topic_elements = soup.select('div[data-component="topic-list"] ul li a')
    if topic_elements:
        topics = [elem.getText().strip() for elem in topic_elements]
        return topics
    else:
        print('No related topics found.')
        return None

def write_bbc_articles_to_csv(**context):
    task_instance = context['task_instance']
    articles = task_instance.xcom_pull(task_ids='scrape_articles')

    article = context['task_instance'].xcom_pull(task_ids='scrape_article_task')
    #articles = scrape_bbc_articles()
    date_str = datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join(os.getcwd(), 'scrapped_data')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    file_path = os.path.join(folder_path, f'bbc_articles_{date_str}.csv')
    
    # Check if file already exists and remove it
    if os.path.isfile(file_path):
        os.remove(file_path)

    # Write articles to CSV file
    with open(file_path, mode='w', encoding='utf-8') as csv_file:
        fieldnames = ['Title', 'Date', 'Category', 'Subcategory', 'Topic', 'Authors', 'Text', 'Images']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for article in articles:
            writer.writerow(article)
    print(f"CSV file written to {file_path}")

scrape_articles_task = PythonOperator(
    task_id='scrape_articles',
    python_callable=scrape_bbc_articles,
    dag=dag
)

write_csv_task = PythonOperator(
    task_id='write_csv',
    python_callable=write_bbc_articles_to_csv,
    dag=dag
)

scrape_articles_task >> write_csv_task