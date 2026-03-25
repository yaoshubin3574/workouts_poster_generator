import argparse
from pathlib import Path
import duckdb
import re
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

# --- 接收 GitHub Actions 传来的参数 ---
parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument('--lat', type=float, required=True, help="中心点纬度")
parser.add_argument('--lon', type=float, required=True, help="中心点经度")
parser.add_argument('--distance', type=int, required=True, help="范围(米)")
parser.add_argument('--city', type=str, required=True, help="城市")
parser.add_argument('--province', type=str, required=True, help="省份")
args = parser.parse_args()
# -----------------------------------

def decode_polyline(polyline_str):
    if not polyline_str: return []
    index, lat, lng = 0, 0, 0
    coordinates = []
    changes = {'latitude': 0, 'longitude': 0}
    while index < len(polyline_str):
        for unit in ['latitude', 'longitude']:
            shift, result = 0, 0
            while True:
                byte = ord(polyline_str[index]) - 63
                index += 1
                result |= (byte & 0x1f) << shift
                shift += 5
                if not byte >= 0x20:
                    break
            if (result & 1):
                changes[unit] = ~(result >> 1)
            else:
                changes[unit] = (result >> 1)
        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates

print(f"步骤 1/3：正在生成 {args.distance}m 范围的 {args.city} SVG 基础地图...")

# 1. 生成干净的 SVG 底图
result = generate_poster(
    PosterRequest(
        output=Path("./base-map"), # 相对路径
        formats=("svg",), 
        lat=args.lat,            # 🌟 动态参数
        lon=args.lon,            # 🌟 动态参数
        title=args.city,         # 🌟 动态参数
        subtitle=args.province,  # 🌟 动态参数
        theme="dark",   
        width_cm=21,
        height_cm=29.7,
        distance_m=args.distance,# 🌟 动态参数
        include_buildings=True,
    )
)

print("步骤 2/3：读取并解密运动数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)
project_func = getattr(projector, 'project', getattr(projector, 'lat_lon_to_pixel', getattr(projector, 'lon_lat_to_pixel', None)))

parquet_path = "data.parquet" # 相对路径
sql = f"SELECT summary_polyline, type FROM read_parquet('{parquet_path}') WHERE summary_polyline IS NOT NULL"

with duckdb.connect() as conn:
    try:
        raw_rows = conn.execute(sql).fetchall()
    except duckdb.BinderException:
        fallback_sql = f"SELECT summary_polyline FROM read_parquet('{parquet_path}') WHERE summary_polyline IS NOT NULL"
        raw_rows = [(r[0], "Unknown") for r in conn.execute(fallback_sql).fetchall()]

print("步骤 3/3：注入矢量轨迹...")

color_map = {
    'Run': '#FC4C02',       
    'Cycling': '#00DFD8',   
    'Ride': '#00DFD8',      
    'Hike': '#FFC300',      
    'Walk': '#A855F7',      
}
default_color = '#06D6A0'   
line_width = max(width_px * 0.0005, 0.75) 

run_routes = []
other_routes = []

for row in raw_rows:
    poly_str = row[0]
    m_type = row[1] if len(row) > 1 else "Unknown"
    lon_lat_list = decode_polyline(poly_str)
    if not lon_lat_list or len(lon_lat_list) < 2:
        continue
    if m_type == 'Run':
        run_routes.append((lon_lat_list, m_type))
    else:
        other_routes.append((lon_lat_list, m_type))

svg_injection_lines = [
    '<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round" opacity="0.95">'
]

def add_route_to_svg(lon_lat_list, m_type):
    pixel_points = []
    for point in lon_lat_list:
        lon, lat = point[0], point[1]
        if project_func.__name__ == 'lat_lon_to_pixel':
            x, y = project_func(lat, lon)
        else:
            x, y = project_func(lon, lat)
        pixel_points.append(f"{x:.1f},{y:.1f}")
    color = color_map.get(m_type, default_color)
    pts_str = " ".join(pixel_points)
    svg_injection_lines.append(f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{line_width:.1f}" />')

for r, t in other_routes:
    add_route_to_svg(r, t)
for r, t in run_routes:
    add_route_to_svg(r, t)
svg_injection_lines.append('</g>')

base_svg_path = result.files[0]
with open(base_svg_path, 'r', encoding='utf-8') as f:
    svg_content = f.read()

# 终极微调区
text_blocks = re.findall(r'<text\b.*?</text>', svg_content, flags=re.IGNORECASE | re.DOTALL)
for block in text_blocks:
    if args.city not in block and args.province not in block:
        svg_content = svg_content.replace(block, '')

dark_glass = '<rect width="100%" height="100%" fill="#050505" opacity="0.5" />\n'
if '<text' in svg_content:
    svg_content = svg_content.replace('<text', dark_glass + '<text', 1)
else:
    svg_injection_lines.insert(0, dark_glass)

SHIFT_Y = 60         
TITLE_SCALE = 0.85   
SUBTITLE_SCALE = 1.4 

def add_translate(tag_str):
    if 'transform="' in tag_str:
        return re.sub(r'transform="([^"]+)"', rf'transform="\1 translate(0, {SHIFT_Y})"', tag_str)
    else:
        if tag_str.endswith('/>'):
            return tag_str.replace('/>', f' transform="translate(0, {SHIFT_Y})"/>', 1)
        else:
            return tag_str.replace('>', f' transform="translate(0, {SHIFT_Y})">', 1)

svg_content = re.sub(rf'<text\b[^>]*>{args.city}</text>', lambda m: add_translate(re.sub(r'font-size="([\d.]+)"', lambda m2: f'font-size="{float(m2.group(1)) * TITLE_SCALE:.1f}"', m.group(0))), svg_content)
svg_content = re.sub(rf'<text\b[^>]*>{args.province}</text>', lambda m: add_translate(re.sub(r'font-size="([\d.]+)"', lambda m2: f'font-size="{float(m2.group(1)) * SUBTITLE_SCALE:.1f}"', m.group(0))), svg_content)
svg_content = re.sub(r'<line\b[^>]*>', lambda m: add_translate(m.group(0)), svg_content)

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(svg_injection_lines) + "\n</svg>")

final_path = "colorful-map.svg" # 相对路径
with open(final_path, 'w', encoding='utf-8') as f:
    f.write(svg_content)

print(f"\n大功告成！海报已生成：{final_path}")
