# 中文 IPTV 筛选工具

这个工具会从公开 IPTV 数据源里收集候选频道，自动筛出当前可播的中文频道，并生成可直接导入播放器的 `m3u` 播放列表。

默认目标比较聚焦：

- `央视`
- `中国大陆卫视`
- 部分已补充的省级子频道
  - `辽宁台`
  - `上海台`
  - `湖南台`
  - `江苏台`
  - `广东台`

频道名、分组名会尽量统一成中文，适合直接导入 APTV 一类播放器使用。
它只处理已经公开暴露的播放列表和频道索引，不抓私有服务，也不绕过付费或权限限制。

## 功能说明

- 从公开数据源拉取候选流，包括 `iptv-org` 和若干公开中文 `m3u`
- 自动收窄到目标频道范围，只保留需要的中文频道
- 把频道名和分组统一成中文，例如 `央视`、`卫视`、`辽宁台`
- 对每个链接做 HTTP/HLS 探测，尽量剔除失效、静态文件和假回退流
- 对常见慢源做更宽容的判断，避免把“能看但首开慢”的源误删
- 支持内联 `User-Agent` / `Referrer`
- 输出整理后的 `m3u` 文件和完整 JSON 报告

## 运行环境

- macOS 或 Linux
- Python 3.10+
- 可选：`ffprobe`
  - 用来做更深一层的媒体流校验
  - 安装方式：`brew install ffmpeg`

## 快速开始

```bash
cd /Users/zhoudali/Desktop/iptv
python3 find_cn_streams.py --verbose
```

默认会生成两个文件：

- `output/chinese-public-verified.m3u`
- `output/chinese-public-report.json`

默认策略会做稳定性检查，并优先保留更可靠的候选源。
脚本默认不偏好裸 IP 源，但当前也会保留一部分已经实测可用的手工优选源。

## 常用命令

全量更激进地扫描：

```bash
python3 find_cn_streams.py --limit 0 --workers 32 --timeout 10 --verbose
```

放宽稳定性过滤：

```bash
python3 find_cn_streams.py --stability-checks 1 --retries 0 --verbose
```

只保留高清及以上候选：

```bash
python3 find_cn_streams.py --min-quality 720 --verbose
```

如果本机装了 `ffprobe`，可以启用更深校验：

```bash
python3 find_cn_streams.py --ffprobe --limit 150 --verbose
```

合并你自己的本地 `m3u` 一起筛：

```bash
python3 find_cn_streams.py --local-m3u /path/to/your.m3u --verbose
```

## 输出内容

`output/chinese-public-verified.m3u`

- 可直接导入播放器
- 保留频道名、分组、必要的 `User-Agent` / `Referrer`

`output/chinese-public-report.json`

- 包含成功和失败的探测结果
- 适合排查某个频道为什么没进最终列表

## GitHub 订阅

仓库里会额外保留一份可直接订阅的 `m3u` 文件：

- `m3u/chinese-public-verified.m3u`

可直接用于 APTV 远程订阅：

- `https://raw.githubusercontent.com/jonhncatt/iptv-cn-finder/main/m3u/chinese-public-verified.m3u`

仓库已配置 GitHub Actions 定时刷新，默认每 6 小时自动运行一次，也支持手动触发。

手动跑一次的方法：

1. 打开仓库的 `Actions` 页面
2. 选择 `Update IPTV Playlist`
3. 点击 `Run workflow`
4. 选择 `main` 分支并确认运行

如果你本机装了 `gh` 命令，也可以在仓库目录执行：

```bash
gh workflow run "Update IPTV Playlist"
```

## 说明

- 公开 IPTV 源变化很快，有些台会失效、限地区，或者只在部分网络下可播。
- 有些源虽然测速偏慢，但播放器里实际仍然能播，所以当前脚本对“慢但可播”的情况做了更宽容的处理。
- 如果某个频道在你本地能播但脚本没收进去，可以把台名告诉我，再继续补优先源或调整规则。
