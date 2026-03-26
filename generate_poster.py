import argparse
from pathlib import Path
import duckdb
import re
import math
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument('--lat', type=float, required=True, help="中心点纬度")
parser.add_argument('--lon', type=float, required=True, help="中心点经度")
parser.add_argument('--distance', type=int, required=True, help="范围(米)")
parser.add_argument('--city', type=str, required=True, help="城市")
args = parser.parse_args()

def parse_time(val):
    if val is None: return 0.0
    if isinstance(val, (int, float)): return float(val)
    val_str = str(val).strip()
    if ' ' in val_str: val_str = val_str.split(' ')[-1]
    try:
        parts = val_str.split(':')
        if len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        return float(val_str)
    except ValueError: return 0.0

def safe_float(val):
    if val is None: return 0.0
    try: return float(val)
    except ValueError: return 0.0

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
                if not byte >= 0x20: break
            if (result & 1): changes[unit] = ~(result >> 1)
            else: changes[unit] = (result >> 1)
        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates

def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

print(f"步骤 1/3：正在生成 {args.distance}m 范围的基础地图...")

result = generate_poster(
    PosterRequest(
        output=Path("./base-map"),
        formats=("svg",), 
        lat=args.lat,  
        lon=args.lon, 
        title="",        
        subtitle="",     
        theme="dark",   
        width_cm=21,
        height_cm=29.7,  
        distance_m=args.distance, 
        include_buildings=True,
    )
)

print("步骤 2/3：读取并汇总云端运动数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)
project_func = getattr(projector, 'project', getattr(projector, 'lat_lon_to_pixel', getattr(projector, 'lon_lat_to_pixel', None)))

sql = """
SELECT 
    summary_polyline, type, distance, moving_time, average_heartrate, elevation_gain 
FROM read_parquet('data.parquet') 
WHERE summary_polyline IS NOT NULL
"""

with duckdb.connect() as conn:
    try:
        raw_rows = conn.execute(sql).fetchall()
        clean_rows = []
        for r in raw_rows:
            clean_rows.append((
                str(r[0]), str(r[1]),
                safe_float(r[2]), parse_time(r[3]), safe_float(r[4]), safe_float(r[5])
            ))
        raw_rows = clean_rows
    except Exception as e:
        print(f"⚠️ 读取统计数据失败 ({e})，部分数据可能显示为0。")
        fallback_sql = "SELECT summary_polyline, type FROM read_parquet('data.parquet') WHERE summary_polyline IS NOT NULL"
        fallback_rows = conn.execute(fallback_sql).fetchall()
        raw_rows = [(str(r[0]), str(r[1]), 0.0, 0.0, 0.0, 0.0) for r in fallback_rows]

print("步骤 3/3：注入矢量轨迹与全新排版统计面板...")

color_map = {
    'Run': '#FC4C02', 'Cycling': '#00DFD8', 'Ride': '#00DFD8',
    'Hike': '#FFC300', 'Walk': '#A855F7'
}
default_color = '#06D6A0'   
line_width = max(width_px * 0.0005, 0.75) 

run_count = ride_count = hike_count = total_count = 0
run_dist_m = ride_dist_m = hike_dist_m = total_dist_m = 0
total_elev_g = total_weighted_hr = total_time_s = 0

run_routes, other_routes = [], []

for row in raw_rows:
    poly_str, m_type, dist_m, time_s, avg_hr, elev_g = row
    decoded_points = decode_polyline(poly_str)
    if not decoded_points or len(decoded_points) < 2: continue
        
    in_region = False
    for point in decoded_points:
        if haversine(point[0], point[1], args.lon, args.lat) <= args.distance:
            in_region = True
            break
            
    if not in_region: continue

    if m_type == 'Run':
        run_routes.append((decoded_points, m_type))
        run_count += 1; run_dist_m += dist_m
    else:
        other_routes.append((decoded_points, m_type))
        if m_type in ['Cycling', 'Ride']: ride_count += 1; ride_dist_m += dist_m
        elif m_type == 'Hike': hike_count += 1; hike_dist_m += dist_m
            
    total_count += 1
    total_dist_m += dist_m
    total_elev_g += elev_g
    total_weighted_hr += avg_hr * time_s
    total_time_s += time_s

run_dist_km = run_dist_m / 1000.0
ride_dist_km = ride_dist_m / 1000.0
hike_dist_km = hike_dist_m / 1000.0
total_dist_km = total_dist_m / 1000.0
total_avg_hr = total_weighted_hr / total_time_s if total_time_s > 0 else 0
total_time_h = int(total_time_s // 3600)
total_time_m = int((total_time_s % 3600) // 60)

svg_injection_lines = ['<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round" opacity="0.95">']

def add_route_to_svg(lon_lat_list, m_type):
    pixel_points = []
    for point in lon_lat_list:
        lon, lat = point[0], point[1]
        x, y = project_func(lat, lon) if project_func.__name__ == 'lat_lon_to_pixel' else project_func(lon, lat)
        pixel_points.append(f"{x:.1f},{y:.1f}")
    color = color_map.get(m_type, default_color)
    pts_str = " ".join(pixel_points)
    svg_injection_lines.append(f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{line_width:.1f}" />')

for r, t in other_routes: add_route_to_svg(r, t)
for r, t in run_routes: add_route_to_svg(r, t)
svg_injection_lines.append('</g>')

with open(result.files[0], 'r', encoding='utf-8') as f:
    svg_content = f.read()

# 强制灰度化底层所有带有黄/棕色的地图线条
def color_to_gray(match):
    val = match.group(1)
    try:
        if len(val) == 3:
            r, g, b = int(val[0], 16)*17, int(val[1], 16)*17, int(val[2], 16)*17
        else:
            r, g, b = int(val[0:2], 16), int(val[2:4], 16), int(val[4:6], 16)
        lum = int((0.299 * r + 0.587 * g + 0.114 * b) * 0.7)
        return f'#{lum:02x}{lum:02x}{lum:02x}'
    except:
        return f'#{val}'

def rgb_to_gray(match):
    try:
        r, g, b = int(match.group(1)), int(match.group(2)), int(match.group(3))
        lum = int((0.299 * r + 0.587 * g + 0.114 * b) * 0.7)
        return f'rgb({lum},{lum},{lum})'
    except:
        return match.group(0)

svg_content = re.sub(r'#([a-fA-F0-9]{6}|[a-fA-F0-9]{3})\b', color_to_gray, svg_content)
svg_content = re.sub(r'rgb\((\d+),\s*(\d+),\s*(\d+)\)', rgb_to_gray, svg_content)

# 净化底层：一键抹除原生文字和线条
svg_content = re.sub(r'<text\b.*?</text>', '', svg_content, flags=re.IGNORECASE | re.DOTALL)
svg_content = re.sub(r'<line\b.*?>', '', svg_content, flags=re.IGNORECASE | re.DOTALL)

# ==========================================
# 💥 全新排版：居中大标题 + 竖线分割看板 💥
# ==========================================
# 滤镜层
dark_glass = '<rect width="100%" height="100%" fill="#050505" opacity="0.5" />\n'

# 整体下移至 0.82 比例 (越接近1越靠下)
stats_y_pos = height_px * 0.82

# 绝对定位的城市标题 (居中并在数据看板正上方)
city_title_block = f'<text x="{width_px / 2:.1f}" y="{stats_y_pos - 120:.1f}" font-family="Arial, Helvetica, sans-serif" font-size="{width_px * 0.045:.1f}" font-weight="bold" fill="#f0f0f0" letter-spacing="16" text-anchor="middle" opacity="0.9">{args.city.upper()}</text>\n'

# 数据大看板 (增加半透明竖线分割，整体下沉)
stats_block = (
    f'<g id="stats_block" transform="translate({width_px/2:.1f}, {stats_y_pos:.1f})" fill="#f0f0f0" font-family="Arial, Helvetica, sans-serif" font-size="60" text-anchor="middle">\n'
    
    # --- 第一行: Runs, Rides, Hikes ---
    f'  <g transform="translate(-380, 0)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{run_count}</tspan><tspan xml:space="preserve"> Runs</tspan>\n'
    f'      <tspan x="0" dy="80" font-weight="bold">{run_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'

    # 分割线 1
    f'  <line x1="-190" y1="-20" x2="-190" y2="90" stroke="#f0f0f0" stroke-width="4" opacity="0.25" stroke-linecap="round" />\n'

    f'  <g transform="translate(0, 0)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{ride_count}</tspan><tspan xml:space="preserve"> Rides</tspan>\n'
    f'      <tspan x="0" dy="80" font-weight="bold">{ride_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'

    # 分割线 2
    f'  <line x1="190" y1="-20" x2="190" y2="90" stroke="#f0f0f0" stroke-width="4" opacity="0.25" stroke-linecap="round" />\n'

    f'  <g transform="translate(380, 0)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{hike_count}</tspan><tspan xml:space="preserve"> Hikes</tspan>\n'
    f'      <tspan x="0" dy="80" font-weight="bold">{hike_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'

    # --- 第二行: BPM, Elev ---
    f'  <g transform="translate(-190, 200)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{int(total_avg_hr)}</tspan><tspan xml:space="preserve"> BPM</tspan>\n'
    f'      <tspan x="0" dy="65" font-size="45" opacity="0.9">Avg Heart Rate</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'

    # 分割线 3
    f'  <line x1="0" y1="180" x2="0" y2="275" stroke="#f0f0f0" stroke-width="4" opacity="0.25" stroke-linecap="round" />\n'

    f'  <g transform="translate(190, 200)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{int(total_elev_g)}</tspan><tspan xml:space="preserve"> m</tspan>\n'
    f'      <tspan x="0" dy="65" font-size="45" opacity="0.9">Elevation Gain</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'

    # --- 第三行: Total 汇总 ---
    f'  <g transform="translate(0, 400)">\n'
    f'    <text>\n'
    f'      <tspan font-weight="bold">{total_count}</tspan><tspan xml:space="preserve"> Workouts Total </tspan><tspan font-weight="bold">{total_dist_km:.1f}</tspan><tspan xml:space="preserve"> km / </tspan><tspan font-weight="bold">{total_time_h}</tspan><tspan xml:space="preserve"> h </tspan><tspan font-weight="bold">{total_time_m}</tspan><tspan xml:space="preserve"> min</tspan>\n'
    f'    </text>\n'
    f'  </g>\n'
    f'</g>\n'
)

final_injection = [
    dark_glass,
    "\n".join(svg_injection_lines),
    city_title_block,
    stats_block
]

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(final_injection) + "\n</svg>")

final_path = "colorful-map.svg"
with open(final_path, 'w', encoding='utf-8') as f:
    f.write(svg_content)

print(f"\n大功告成！海报已生成：{final_path}")
