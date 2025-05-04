import requests
from bs4 import BeautifulSoup
import csv
import os
import subprocess
import logging
import time
import random
import re
import hashlib
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 配置日志记录，包含调试信息
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_debug.log'),
        logging.StreamHandler()
    ]
)

# 从环境变量获取设置
forum_url = os.getenv("FORUM_URL", "https://pornotorrent.top/forum-1670/")
forum_id = os.getenv("FORUM_ID", "1670")
csv_file = os.getenv("CSV_FILE", f"{forum_id}.csv")
base_url = forum_url.rstrip('/') + '/'
download_base_url = "https://files.cdntraffic.top/PL/torrent/files/"
MAX_RETRIES = 3
RETRY_DELAY = 0.5
COMMIT_INTERVAL = 500
TIMEOUT = 10
MAX_WORKERS = 5

# 请求头配置
page_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Referer": "https://pornotorrent.top/"
}

torrent_headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,zh-CN;q=0.9,zh;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="135", "Not-A.Brand";v="8"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Priority": "u=0, i"
}

# 配置请求会话，带重试机制
session = requests.Session()
retries = Retry(total=MAX_RETRIES, backoff_factor=RETRY_DELAY, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

def clean_title(title):
    """清洗标题，仅保留英文部分，去除斜杠"""
    try:
        # 按斜杠分割标题，取第一个有效部分
        parts = [part.strip() for part in title.split('/')]
        logging.debug(f"标题分割: 原始='{title}', 分割后={parts}")
        
        # 优先选择包含字母的最长英文部分
        valid_parts = []
        for part in parts:
            # 匹配包含字母、数字、空格、标点，但不包含斜杠
            match = re.match(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+$', part)
            if match:
                cleaned = match.group(0).strip()
                if len(cleaned) > 3 or not re.match(r'^(ART|720p|1080p)$', cleaned):  # 排除短标签
                    valid_parts.append(cleaned)
        
        if valid_parts:
            # 选择最长的有效部分
            cleaned = max(valid_parts, key=len)
            logging.debug(f"清洗标题: 原始='{title}', 清洗后='{cleaned}'")
            return cleaned
        
        # 如果没有有效英文部分，尝试提取所有英文字符
        match = re.search(r'[A-Za-z0-9\s.,:;!?\'\"()\-+&]+', title)
        if match:
            cleaned = match.group(0).strip()
            if len(cleaned) > 3 or not re.match(r'^(ART|720p|1080p)$', cleaned):
                logging.debug(f"回退清洗: 原始='{title}', 清洗后='{cleaned}'")
                return cleaned
        
        # 如果仍无效，保留原始标题并警告
        logging.warning(f"标题无有效英文部分，保留原始: {title}")
        return title
    except Exception as e:
        logging.error(f"清洗标题失败: {title}, 错误: {e}")
        return title

def init_csv():
    """初始化CSV文件，仅当文件不存在时创建"""
    try:
        if not os.path.exists(csv_file):
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"新建CSV文件: {csv_file}")
        else:
            file_size = os.path.getsize(csv_file)
            with open(csv_file, 'r', encoding='utf-8') as file:
                line_count = sum(1 for line in file)
            logging.info(f"CSV文件已存在: {csv_file}, 大小: {file_size}字节, 行数: {line_count}")
    except Exception as e:
        logging.error(f"初始化CSV文件失败: {e}")
        raise

def configure_git_lfs():
    """配置Git LFS跟踪"""
    try:
        result = subprocess.run(["git", "lfs", "track", csv_file], check=True, capture_output=True, text=True)
        logging.debug(f"Git LFS配置成功: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git LFS配置失败: {e.stderr}")
        raise

def git_commit(message):
    """提交CSV文件到Git仓库"""
    logging.info(f"准备提交: {message}")
    try:
        result_add = subprocess.run(["git", "add", csv_file], capture_output=True, text=True, check=True)
        logging.debug(f"Git add 输出: {result_add.stdout}, 错误: {result_add.stderr}")
        
        result_commit = subprocess.run(["git", "commit", "-m", message], capture_output=True, text=True)
        logging.debug(f"Git commit 输出: {result_commit.stdout}, 错误: {result_commit.stderr}")
        
        if result_commit.returncode == 0:
            result_push = subprocess.run(["git", "push"], capture_output=True, text=True, check=True)
            logging.info(f"Git提交成功: {message}")
            logging.debug(f"Git push 输出: {result_push.stdout}, 错误: {result_push.stderr}")
        else:
            logging.warning(f"无更改需要提交: {result_commit.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git操作失败: {e.stderr}")
        raise

def get_topic_id(url):
    """从URL提取话题ID"""
    match = re.search(r'/(\d+)-t\.html$', url)
    topic_id = match.group(1) if match else None
    logging.debug(f"提取话题ID: URL={url}, ID={topic_id}")
    return topic_id

def get_download_url(topic_id):
    """构造下载URL"""
    download_url = f"{download_base_url}{topic_id}.torrent" if topic_id else ""
    logging.debug(f"构造下载URL: topic_id={topic_id}, URL={download_url}")
    return download_url

def torrent_to_magnet(torrent_url):
    """将torrent URL转换为magnet链接"""
    logging.debug(f"尝试转换torrent到magnet: {torrent_url}")
    try:
        response = session.get(torrent_url, headers=torrent_headers, timeout=TIMEOUT)
        response.raise_for_status()
        torrent_content = response.content
        info_hash = hashlib.sha1(torrent_content).hexdigest()
        magnet = f"magnet:?xt=urn:btih:{info_hash}"
        logging.debug(f"成功转换为magnet: {magnet}")
        return magnet
    except Exception as e:
        logging.warning(f"转换torrent失败: {torrent_url}, 错误: {e}")
        return torrent_url

def get_max_page():
    """从论坛首页提取最大页数"""
    logging.info(f"正在获取最大页数: {base_url}")
    try:
        response = session.get(base_url, headers=page_headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_links = soup.select('#pagination a[href*="/page/"]')
        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            match = re.search(r'/page/(\d+)/', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)
        
        logging.info(f"提取到最大页数: {max_page}")
        return max_page
    except Exception as e:
        logging.error(f"提取最大页数失败: {e}")
        logging.warning("默认使用页数=1")
        return 1

def crawl_page(page_number, retries=0):
    """爬取单页数据"""
    try:
        if page_number == 1:
            url = base_url
        else:
            url = f"{base_url}page/{page_number}/"
        
        logging.info(f"正在爬取页面 {page_number}: {url}")
        response = session.get(url, headers=page_headers, timeout=TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        torrent_rows = soup.select('tr[id^="tr-"]')
        if not torrent_rows:
            logging.warning(f"页面 {page_number} 未找到torrent行，检查选择器 'tr[id^=\"tr-\"]'")
            return []
        
        results = []
        for row in torrent_rows:
            try:
                title_elem = row.select_one('a.torTopic.bold.tt-text')
                if not title_elem:
                    logging.debug(f"页面 {page_number} 的行缺少标题元素，跳过")
                    continue
                raw_title = title_elem.get_text(strip=True)
                title = clean_title(raw_title)
                
                topic_url = urljoin(base_url, title_elem['href'])
                
                topic_id = get_topic_id(topic_url)
                download_url = get_download_url(topic_id)
                link = torrent_to_magnet(download_url) if download_url else ""
                
                publisher_elem = row.select_one('div.topicAuthor a.topicAuthor')
                publisher = publisher_elem.get_text(strip=True) if publisher_elem else "Unknown"
                
                result = {
                    "Page": page_number,
                    "Title": title,
                    "URL": topic_url,
                    "Publisher": publisher,
                    "Link": link
                }
                results.append(result)
                logging.debug(f"页面 {page_number} 添加记录: {title}")
                
            except Exception as e:
                logging.error(f"处理页面 {page_number} 的行时出错: {e}")
                continue
        
        logging.info(f"页面 {page_number}: 找到 {len(results)} 条记录")
        return results
    
    except requests.RequestException as e:
        if retries < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** retries)
            logging.warning(f"页面 {page_number} 重试 {retries + 1}/{MAX_RETRIES}，等待 {delay}秒: {e}")
            time.sleep(delay)
            return crawl_page(page_number, retries + 1)
        logging.error(f"爬取页面 {page_number} 失败，尝试 {MAX_RETRIES} 次: {e}")
        return []

def crawl_pages(start_page, end_page):
    """主爬取逻辑"""
    logging.info(f"开始爬取，从页面 {start_page} 到 {end_page}")
    try:
        configure_git_lfs()
        
        if start_page == 0:
            logging.info("start_page为0，清空CSV并提取最大页数")
            with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["Page", "Title", "URL", "Publisher", "Link"])
            logging.info(f"已清空CSV文件: {csv_file}")
            start_page = get_max_page()
            logging.info(f"设置start_page为 {start_page}")
        
        init_csv()
        
        total_records = 0
        pages = list(range(start_page, end_page - 1, -1))
        logging.debug(f"页面列表: {pages}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {executor.submit(crawl_page, page): page for page in pages}
            
            for future in tqdm(future_to_page, desc="爬取页面", total=len(pages)):
                page_number = future_to_page[future]
                try:
                    results = future.result()
                    logging.debug(f"页面 {page_number} 返回 {len(results)} 条记录")
                    if results:
                        try:
                            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                for data in results:
                                    writer.writerow([data["Page"], data["Title"], data["URL"], 
                                                  data["Publisher"], data["Link"]])
                                    total_records += 1
                                logging.info(f"页面 {page_number}: 写入 {len(results)} 条记录到 {csv_file}")
                        except Exception as e:
                            logging.error(f"写入CSV失败，页面 {page_number}: {e}")
                            continue
                    else:
                        logging.warning(f"页面 {page_number}: 无数据写入，记录为空")
                    
                    logging.debug(f"当前累计记录数: {total_records}")
                    if total_records >= COMMIT_INTERVAL:
                        logging.info(f"达到提交间隔 {COMMIT_INTERVAL}，提交记录")
                        git_commit(f"更新 {total_records} 条记录至页面 {page_number}")
                        total_records = 0
                            
                except Exception as e:
                    logging.error(f"处理页面 {page_number} 时出错: {e}")
                    continue
                
                time.sleep(random.uniform(0.5, 1.5))
        
        if total_records > 0:
            logging.info(f"最后提交剩余 {total_records} 条记录")
            git_commit(f"最后更新剩余 {total_records} 条记录")
        
        file_size = os.path.getsize(csv_file)
        with open(csv_file, 'r', encoding='utf-8') as file:
            line_count = sum(1 for line in file)
        logging.info(f"最终CSV状态: {csv_file}, 大小: {file_size}字节, 行数: {line_count}")
    
    except Exception as e:
        logging.error(f"crawl_pages 中发生未预期错误: {e}")
        raise
    finally:
        logging.info("完成爬取流程")

if __name__ == "__main__":
    logging.info("脚本启动")
    try:
        session.get("https://pornotorrent.top/", headers=page_headers, timeout=TIMEOUT)
        logging.info("已初始化会话")
    except requests.RequestException as e:
        logging.warning(f"初始化会话失败: {e}")
    
    start_page = int(os.getenv("START_PAGE", 283))
    end_page = int(os.getenv("END_PAGE", 1))
    logging.info(f"开始爬取论坛 {forum_id}，从页面 {start_page} 到 {end_page}")
    crawl_pages(start_page, end_page)
    logging.info(f"数据已保存至 {csv_file}")
    session.close()
    logging.info("脚本结束")
