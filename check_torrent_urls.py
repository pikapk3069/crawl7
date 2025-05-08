import aiohttp
import asyncio
import logging
import os
import subprocess
import time
from urllib.parse import urljoin

# 配置日志记录，包含调试信息
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("check_torrent_urls.log"), logging.StreamHandler()],
)

# 文件路径
INPUT_FILE = "torrent_error.txt"
OK_FILE = "torrent_check_ok.txt"
ERROR_FILE = "torrent_check_error.txt"
TIMEOUT = 5
MAX_CONCURRENT = 10
COMMIT_INTERVAL = 100
WRITE_BATCH_SIZE = 100  # 每 100 个 URL 写入一次文件

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
}


def configure_git_lfs():
    """配置 Git LFS 跟踪输出文件"""
    try:
        for file in [OK_FILE, ERROR_FILE]:
            result = subprocess.run(
                ["git", "lfs", "track", file],
                check=True,
                capture_output=True,
                text=True,
            )
            logging.debug(f"Git LFS 配置成功: {file}, 输出: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git LFS 配置失败: {e.stderr}")
        raise


def git_commit(message):
    """提交文件到 Git 仓库"""
    logging.info(f"准备提交: {message}")
    try:
        for file in [OK_FILE, ERROR_FILE]:
            result_add = subprocess.run(
                ["git", "add", file], capture_output=True, text=True, check=True
            )
            logging.debug(
                f"Git add {file} 输出: {result_add.stdout}, 错误: {result_add.stderr}"
            )

        result_commit = subprocess.run(
            ["git", "commit", "-m", message], capture_output=True, text=True
        )
        logging.debug(
            f"Git commit 输出: {result_commit.stdout}, 错误: {result_commit.stderr}"
        )

        if result_commit.returncode == 0:
            result_push = subprocess.run(
                ["git", "push"], capture_output=True, text=True, check=True
            )
            logging.info(f"Git 提交成功: {message}")
            logging.debug(
                f"Git push 输出: {result_push.stdout}, 错误: {result_push.stderr}"
            )
        else:
            logging.warning(f"无更改需要提交: {result_commit.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Git 操作失败: {e.stderr}")
        raise


def write_results(ok_urls, error_urls):
    """将内存中的结果写入文件"""
    try:
        with open(OK_FILE, "a", encoding="utf-8") as f:
            f.write("\n".join(ok_urls) + "\n" if ok_urls else "")
        with open(ERROR_FILE, "a", encoding="utf-8") as f:
            f.write(
                "\n".join(f"{url} (状态码: {status})" for url, status in error_urls)
                + "\n"
                if error_urls
                else ""
            )
        logging.info(
            f"写入文件: {OK_FILE} ({len(ok_urls)} 条), {ERROR_FILE} ({len(error_urls)} 条)"
        )
    except Exception as e:
        logging.error(f"写入文件失败: {e}")
        raise


async def check_url(session, url):
    """异步检查单个 URL 是否可下载"""
    try:
        async with session.head(
            url.strip(), timeout=TIMEOUT, allow_redirects=True, headers=HEADERS
        ) as response:
            status = response.status
            logging.debug(f"URL {url} 返回状态码: {status}")
            return url, status
    except Exception as e:
        logging.warning(f"URL {url} 检查失败: {e}")
        return url, 0


async def main():
    """主函数：检查所有 URL 并写入结果"""
    start_time = time.time()
    logging.info(f"开始检查 URL，从文件: {INPUT_FILE}")

    # 读取 URL 列表
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            urls = [line.strip() for line in f if line.strip()]
        logging.info(f"读取到 {len(urls)} 个 URL")
    except Exception as e:
        logging.error(f"读取文件 {INPUT_FILE} 失败: {e}")
        return

    # 初始化输出文件
    try:
        for file in [OK_FILE, ERROR_FILE]:
            with open(file, "w", encoding="utf-8") as f:
                f.write("")
            logging.info(f"初始化文件: {file}")
    except Exception as e:
        logging.error(f"初始化输出文件失败: {e}")
        return

    # 配置 Git LFS
    configure_git_lfs()

    # 异步检查 URL
    ok_urls = []
    error_urls = []
    total_processed = 0

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(urls), MAX_CONCURRENT):
            batch = urls[i : i + MAX_CONCURRENT]
            tasks = [check_url(session, url) for url in batch]
            results = await asyncio.gather(*tasks)

            # 收集结果到内存
            batch_ok = []
            batch_error = []
            for url, status in results:
                if status == 200:
                    batch_ok.append(url)
                else:
                    batch_error.append((url, status))
                total_processed += 1
                logging.debug(f"处理 URL {url}: 状态码 {status}")

            ok_urls.extend(batch_ok)
            error_urls.extend(batch_error)

            # 达到批量写入阈值时写入文件
            if len(ok_urls) + len(error_urls) >= WRITE_BATCH_SIZE:
                write_results(ok_urls, error_urls)
                ok_urls.clear()
                error_urls.clear()

            # 定期提交
            if total_processed >= COMMIT_INTERVAL:
                git_commit(f"检查 {total_processed} 个 URL")
                total_processed = 0

    # 写入剩余结果
    if ok_urls or error_urls:
        write_results(ok_urls, error_urls)

    # 最后提交
    if total_processed > 0:
        git_commit(f"最后检查 {total_processed} 个 URL")

    # 总结
    logging.info(
        f"检查完成，总计 {len(urls)} 个 URL，成功: {len(ok_urls)}, 失败: {len(error_urls)}"
    )
    logging.info(f"成功 URL 写入: {OK_FILE}")
    logging.info(f"失败 URL 写入: {ERROR_FILE}")
    logging.info(f"总耗时: {time.time() - start_time:.2f} 秒")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"脚本执行失败: {e}")
    finally:
        logging.info("脚本结束")
