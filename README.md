# SF Hong Kong Store Map (Local)

本项目是一个本地可运行示例：
- 从顺丰香港网点页提取香港网点
- 为每个网点补充英文地址（优先英文页面与官方接口）
- 坐标优先使用顺丰官方接口（`store/hk/query`）
- 若网络不可用，自动回退为“按行政区中心 + 固定偏移”的离线近似坐标
- 用 Leaflet + OpenStreetMap 在地图上展示

## 1) 构建数据

```bash
cd /Users/bytedance/Downloads/sf-hk-map
python3 scripts/build_data.py
```

可选参数：

```bash
# 重新拉取顺丰页面
python3 scripts/build_data.py --refresh-source

# 跳过顺丰官方坐标接口（强制使用离线近似坐标）
python3 scripts/build_data.py --skip-official-api
```

输出文件：
- `data/sf_hk_stores.json`
- `data/sf_hk_stores.geojson`
- `data/sf_hk_fallback_rows.json`
- `data/sf_hk_store_official_raw.json`（若官方接口请求成功）

## 2) 启动本地网页

```bash
cd /Users/bytedance/Downloads/sf-hk-map
python3 -m http.server 8000
```

浏览器打开：`http://localhost:8000`

## GitHub 定时更新

仓库内已包含工作流：`.github/workflows/update-data.yml`  
会按计划在 GitHub 上定时执行 `scripts/build_data.py`，有数据变化就自动提交到 `main`，并触发 Pages 重新部署。

## 说明

- 地图底图使用 OpenStreetMap，无需付费 API Key。
- 当前示例偏重“本地快速可视化”，不依赖付费服务。
- 若你要公网长期商用，建议接入稳定商业地图与坐标服务。
