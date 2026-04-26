from flask import Flask, render_template, jsonify, request
from werkzeug.utils import secure_filename
import xlrd
import json
import os
import glob
import requests
from datetime import datetime

app = Flask(__name__)

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')
ROLES_FILE = os.path.join(BASE_DIR, 'roles.json')
PLAYER_SETTINGS_FILE = os.path.join(BASE_DIR, 'player_settings.json')
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'credentials.json')

ROLE_OPTIONS = ['EXP', 'JUNGLE', 'MID', 'ROAM', 'GOLD']
ALLOWED_EXTENSIONS = {'.xls', '.xlsx'}

SCOREGG_DATA_ITEMS = ','.join([
    'tournament', 'kills', 'deaths', 'assists', 'kda', 'percent',
    'money', 'min_money', 'team_money', 'team_min_money', 'money_percent',
    'xpm', 'total_damage', 'min_damage', 'team_damage', 'damage_share',
    'damage_gold', 'damage_taken_gold',
    'building_damage', 'building_damage_per_minute', 'team_building_damage', 'building_damage_share',
    'damage_taken', 'damage_taken_per_minute', 'team_damage_taken', 'damage_taken_share',
    'control_time_s', 'heal',
    'lengendary', 'savage', 'maniac', 'triple_kill', 'double_kill', 'first_blood',
    'tower_destroy_count', 'cryoturtle_kill_count', 'lord_kill_count', 'time_s',
])

os.makedirs(DATA_DIR, exist_ok=True)

_orig = os.path.join(BASE_DIR, 'inverto.xls')
_dest = os.path.join(DATA_DIR, 'inverto.xls')
if os.path.exists(_orig) and not os.path.exists(_dest):
    import shutil
    shutil.move(_orig, _dest)


def load_roles():
    if os.path.exists(ROLES_FILE):
        with open(ROLES_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_roles(roles):
    with open(ROLES_FILE, 'w') as f:
        json.dump(roles, f, indent=2)


def load_player_settings():
    if os.path.exists(PLAYER_SETTINGS_FILE):
        with open(PLAYER_SETTINGS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_player_settings(settings):
    with open(PLAYER_SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=2)


def load_credentials():
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_credentials(creds):
    with open(CREDENTIALS_FILE, 'w') as f:
        json.dump(creds, f, indent=2)


def fetch_scoregg_excel(tournament_id, start_date, end_date):
    """Fetch the per-game records Excel from scoregg API."""
    creds = load_credentials()
    token = creds.get('token', '')
    uid = creds.get('uid', '')
    if not token or not uid:
        raise ValueError('No credentials configured. Set token and uid first.')

    url = 'https://mlbb.scoregg.com/services/query/player_record.php'
    params = {
        'gameID': '1',
        'tournamentID_string': str(tournament_id),
        'start_time': start_date,
        'end_time': end_date,
        'playerID': '',
        'heroID': '',
        'teamID': '',
        'min_count': '0',
        'data_items': SCOREGG_DATA_ITEMS,
        'is_download': '1',
        'language': 'en',
    }
    headers = {
        'accept': 'application/json, text/plain, */*',
        'referer': 'https://mlbb.scoregg.com/query-data/query',
        'token': token,
        'uid': str(uid),
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/147.0.0.0 Safari/537.36',
    }
    cookies = {'_token': token, '_uid': str(uid)}

    r = requests.get(url, params=params, headers=headers, cookies=cookies, timeout=60)
    r.raise_for_status()
    content = r.content
    if not content or len(content) < 1024:
        raise ValueError(f'Response too small ({len(content)} bytes), likely an error: {content[:300]!r}')
    if content[:5] in (b'<!DOC', b'<html'):
        raise ValueError('Got HTML instead of Excel — credentials may be expired')
    return content


def get_all_excel_files():
    files = []
    for ext in ALLOWED_EXTENSIONS:
        files.extend(glob.glob(os.path.join(DATA_DIR, f'*{ext}')))
    return sorted(files)


def parse_pct(val):
    """Parse '50%' -> 50.0, or numeric -> float."""
    if isinstance(val, str):
        return float(val.replace('%', '')) if val.strip() else 0
    return float(val) if val else 0


def parse_single_excel(filepath):
    book = xlrd.open_workbook(filepath, ignore_workbook_corruption=True)
    sheet = book.sheet_by_index(0)
    headers = [sheet.cell_value(0, c) for c in range(sheet.ncols)]
    col = {h: i for i, h in enumerate(headers)}

    rows = []
    for r in range(1, sheet.nrows):
        def val(name, _r=r):
            return sheet.cell_value(_r, col[name])

        time_s = val('Time/s')
        time_min = time_s / 60 if time_s else 0
        tournament = val('Tournament') if 'Tournament' in col else ''
        player_code = val('Player Code') if 'Player Code' in col else ''
        kp_raw = val('Kill Participation%') if 'Kill Participation%' in col else 0

        rows.append({
            'game_id': val('Battle Code'),
            'date': val('Date'),
            'map': val('Map'),
            'team': val('Team'),
            'enemy_team': val('Enemy Team'),
            'result': val('Result'),
            'player': val('Player'),
            'player_code': player_code,
            'hero': val('Hero'),
            'kills': int(val('Kills')),
            'deaths': int(val('Deaths')),
            'assists': int(val('Assists')),
            'kda': val('KDA'),
            'kill_participation': parse_pct(kp_raw),
            'gold': val('Gold'),
            'gpm': val('Gold per Minute'),
            'damage': val('Damage'),
            'dpm': val('Damage per Minute'),
            'damage_taken': val('Damage Taken'),
            'damage_taken_pm': val('Damage Taken per Minute'),
            'time_s': time_s,
            'time_min': round(time_min, 1),
            'exp_pm': val('EXP per Minute'),
            'tournament': tournament,
            'source_file': os.path.basename(filepath),
        })
    return rows


def parse_all_excels():
    all_rows = []
    for f in get_all_excel_files():
        try:
            all_rows.extend(parse_single_excel(f))
        except Exception as e:
            print(f"Error parsing {f}: {e}")
    return all_rows


def calc_stats_from_rows(rows):
    if not rows:
        return None
    total_kills = sum(r['kills'] for r in rows)
    total_deaths = sum(r['deaths'] for r in rows)
    total_assists = sum(r['assists'] for r in rows)
    total_damage = sum(r['damage'] for r in rows)
    total_damage_taken = sum(r['damage_taken'] for r in rows)
    gpm_list = [r['gpm'] for r in rows]
    dpm_list = [r['dpm'] for r in rows]
    kp_list = [r['kill_participation'] for r in rows]
    n = len(rows)
    deaths = total_deaths if total_deaths > 0 else 1
    wins = sum(1 for g in rows if g['result'] == 'win')

    return {
        'games': n,
        'wins': wins,
        'winrate': round(wins / n * 100, 1) if n else 0,
        'total_kills': total_kills,
        'total_deaths': total_deaths,
        'total_assists': total_assists,
        'avg_kda': round((total_kills + total_assists) / deaths, 2),
        'avg_kills': round(total_kills / n, 1),
        'avg_deaths': round(total_deaths / n, 1),
        'avg_assists': round(total_assists / n, 1),
        'avg_kp': round(sum(kp_list) / n, 1),
        'avg_gpm': int(round(sum(gpm_list) / n, 0)),
        'best_gpm': int(max(gpm_list)),
        'worst_gpm': int(min(gpm_list)),
        'avg_dpm': int(round(sum(dpm_list) / n, 0)),
        'avg_damage': int(round(total_damage / n, 0)),
        'avg_damage_taken': int(round(total_damage_taken / n, 0)),
        'damage_taken_per_death': int(round(total_damage_taken / deaths, 0)),
        'avg_time_min': round((sum(r['time_s'] for r in rows) / n) / 60, 1),
    }


def compute_player_stats(rows, team_filter=''):
    """Group by player_code. If team_filter is set, only count games where
    the player played for that team, but still show the player."""
    roles = load_roles()
    settings = load_player_settings()
    players = {}

    for row in rows:
        code = row['player_code'] or row['player']
        if code not in players:
            players[code] = {
                'player_code': code,
                'name': row['player'],
                'names': set(),
                'teams': set(),
                'all_rows': [],
                'tournaments': set(),
            }
        p = players[code]
        p['names'].add(row['player'])
        p['teams'].add(row['team'])
        p['all_rows'].append(row)
        p['name'] = row['player']
        if row['tournament']:
            p['tournaments'].add(row['tournament'])

    result = []
    for code, p in players.items():
        all_teams = sorted(p['teams'])
        default_team = settings.get(code, {}).get('default_team', '')
        display_team = default_team if default_team in p['teams'] else all_teams[-1]

        # If team filter active, only use rows where player was on that team
        if team_filter:
            filtered_rows = [r for r in p['all_rows'] if r['team'] == team_filter]
            if not filtered_rows:
                continue  # player never played for this team
        else:
            filtered_rows = p['all_rows']

        stats = calc_stats_from_rows(filtered_rows)
        if not stats:
            continue
        stats['player_code'] = code
        stats['name'] = p['name']
        stats['all_names'] = sorted(p['names'])
        stats['team'] = display_team
        stats['all_teams'] = all_teams
        stats['role'] = roles.get(code, '')
        stats['tournaments'] = sorted(p['tournaments'])
        result.append(stats)

    result.sort(key=lambda x: x['avg_kda'], reverse=True)
    return result


def compute_team_stats(rows):
    teams = {}
    for row in rows:
        t = row['team']
        if t not in teams:
            teams[t] = []
        teams[t].append(row)

    result = []
    for team_name, team_rows in teams.items():
        game_ids = set(r['game_id'] for r in team_rows)
        wins = len(set(r['game_id'] for r in team_rows if r['result'] == 'win'))
        n_games = len(game_ids)
        stats = calc_stats_from_rows(team_rows)
        stats['name'] = team_name
        stats['team_games'] = n_games
        stats['team_wins'] = wins
        stats['team_winrate'] = round(wins / n_games * 100, 1) if n_games else 0
        result.append(stats)

    result.sort(key=lambda x: x['team_winrate'], reverse=True)
    return result


@app.route('/')
def index():
    return render_template('index.html', roles=ROLE_OPTIONS)


@app.route('/compare')
def compare_page():
    return render_template('compare.html', roles=ROLE_OPTIONS)


@app.route('/api/stats')
def api_stats():
    rows = parse_all_excels()
    tournament = request.args.get('tournament', '')
    team = request.args.get('team', '')
    if tournament:
        rows = [r for r in rows if r['tournament'] == tournament]
    stats = compute_player_stats(rows, team_filter=team)
    return jsonify(stats)


@app.route('/api/tournaments')
def api_tournaments():
    rows = parse_all_excels()
    tournaments = sorted(set(r['tournament'] for r in rows if r['tournament']))
    return jsonify(tournaments)


@app.route('/api/teams')
def api_teams():
    rows = parse_all_excels()
    tournament = request.args.get('tournament', '')
    if tournament:
        rows = [r for r in rows if r['tournament'] == tournament]
    stats = compute_team_stats(rows)
    return jsonify(stats)


@app.route('/api/player/<code>')
def api_player(code):
    rows = parse_all_excels()
    player_rows = [r for r in rows if r['player_code'] == code]
    if not player_rows:
        player_rows = [r for r in rows if r['player'] == code]
    if not player_rows:
        return jsonify({'error': 'Player not found'}), 404

    roles = load_roles()
    settings = load_player_settings()
    pc = player_rows[0]['player_code'] or code
    all_names = sorted(set(r['player'] for r in player_rows))
    all_teams = sorted(set(r['team'] for r in player_rows))
    default_team = settings.get(pc, {}).get('default_team', '')

    # Per-tournament breakdown
    tournaments = {}
    for r in player_rows:
        t = r['tournament'] or 'Unknown'
        if t not in tournaments:
            tournaments[t] = []
        tournaments[t].append(r)
    tournament_stats = {}
    for t, t_rows in tournaments.items():
        tournament_stats[t] = calc_stats_from_rows(t_rows)

    # Per-team breakdown
    teams_map = {}
    for r in player_rows:
        t = r['team']
        if t not in teams_map:
            teams_map[t] = []
        teams_map[t].append(r)
    team_stats = {}
    for t, t_rows in teams_map.items():
        team_stats[t] = calc_stats_from_rows(t_rows)

    return jsonify({
        'player_code': pc,
        'name': player_rows[-1]['player'],
        'all_names': all_names,
        'all_teams': all_teams,
        'default_team': default_team,
        'team': default_team if default_team in all_teams else all_teams[-1],
        'role': roles.get(pc, ''),
        'games': player_rows,
        'tournament_stats': tournament_stats,
        'team_stats': team_stats,
    })


@app.route('/api/compare/players')
def compare_players():
    codes = request.args.getlist('code')
    tournament = request.args.get('tournament', '')
    rows = parse_all_excels()
    if tournament:
        rows = [r for r in rows if r['tournament'] == tournament]

    roles = load_roles()
    result = {}
    for code in codes:
        p_rows = [r for r in rows if r['player_code'] == code]
        if p_rows:
            stats = calc_stats_from_rows(p_rows)
            stats['player_code'] = code
            stats['name'] = p_rows[-1]['player']
            stats['team'] = p_rows[-1]['team']
            stats['role'] = roles.get(code, '')
            result[code] = stats

    return jsonify(result)


@app.route('/api/compare/teams')
def compare_teams():
    names = request.args.getlist('name')
    tournament = request.args.get('tournament', '')
    rows = parse_all_excels()
    if tournament:
        rows = [r for r in rows if r['tournament'] == tournament]

    result = {}
    for name in names:
        t_rows = [r for r in rows if r['team'] == name]
        if t_rows:
            game_ids = set(r['game_id'] for r in t_rows)
            wins = len(set(r['game_id'] for r in t_rows if r['result'] == 'win'))
            n_games = len(game_ids)
            stats = calc_stats_from_rows(t_rows)
            stats['name'] = name
            stats['team_games'] = n_games
            stats['team_wins'] = wins
            stats['team_winrate'] = round(wins / n_games * 100, 1) if n_games else 0
            result[name] = stats

    return jsonify(result)


@app.route('/api/role', methods=['POST'])
def set_role():
    data = request.json
    player_code = data.get('player_code')
    role = data.get('role')
    if not player_code:
        return jsonify({'error': 'Player code required'}), 400
    roles = load_roles()
    roles[player_code] = role
    save_roles(roles)
    return jsonify({'ok': True})


@app.route('/api/default_team', methods=['POST'])
def set_default_team():
    data = request.json
    player_code = data.get('player_code')
    team = data.get('team', '')
    if not player_code:
        return jsonify({'error': 'Player code required'}), 400
    settings = load_player_settings()
    if player_code not in settings:
        settings[player_code] = {}
    settings[player_code]['default_team'] = team
    save_player_settings(settings)
    return jsonify({'ok': True})


@app.route('/api/files')
def list_files():
    files = []
    for f in get_all_excel_files():
        name = os.path.basename(f)
        size = os.path.getsize(f)
        try:
            row_count = xlrd.open_workbook(f, ignore_workbook_corruption=True).sheet_by_index(0).nrows - 1
        except Exception:
            row_count = 0
        files.append({'name': name, 'size': size, 'rows': row_count})
    return jsonify(files)


@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({'error': f'Only {", ".join(ALLOWED_EXTENSIONS)} files allowed'}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(DATA_DIR, filename)
    file.save(filepath)

    try:
        parse_single_excel(filepath)
    except Exception as e:
        os.remove(filepath)
        return jsonify({'error': f'Invalid Excel file: {str(e)}'}), 400

    return jsonify({'ok': True, 'filename': filename})


@app.route('/api/files/<filename>', methods=['DELETE'])
def delete_file(filename):
    filepath = os.path.join(DATA_DIR, secure_filename(filename))
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    os.remove(filepath)
    return jsonify({'ok': True})


@app.route('/api/credentials', methods=['GET', 'POST'])
def credentials():
    if request.method == 'GET':
        creds = load_credentials()
        # Don't return the full token - just whether it's set + masked preview
        token = creds.get('token', '')
        return jsonify({
            'has_token': bool(token),
            'token_preview': (token[:6] + '...' + token[-4:]) if len(token) > 10 else '',
            'uid': creds.get('uid', ''),
        })
    data = request.json
    token = (data.get('token') or '').strip()
    uid = (data.get('uid') or '').strip()
    if not token or not uid:
        return jsonify({'error': 'Both token and uid are required'}), 400
    save_credentials({'token': token, 'uid': uid})
    return jsonify({'ok': True})


@app.route('/api/fetch_tournament', methods=['POST'])
def fetch_tournament():
    data = request.json
    tournament_id = (data.get('tournament_id') or '').strip()
    start_date = (data.get('start_date') or '').strip()
    end_date = (data.get('end_date') or '').strip()
    if not tournament_id:
        return jsonify({'error': 'tournament_id is required'}), 400

    # Default to wide range if dates not provided
    if not start_date:
        start_date = '2020-01-01'
    if not end_date:
        end_date = '2030-12-31'

    try:
        content = fetch_scoregg_excel(tournament_id, start_date, end_date)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except requests.HTTPError as e:
        return jsonify({'error': f'HTTP {e.response.status_code} from scoregg'}), 502
    except requests.RequestException as e:
        return jsonify({'error': f'Network error: {e}'}), 502

    filename = f'scoregg_t{tournament_id}.xls'
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'wb') as f:
        f.write(content)

    try:
        rows = parse_single_excel(filepath)
    except Exception as e:
        os.remove(filepath)
        return jsonify({'error': f'Saved file could not be parsed: {e}'}), 500

    return jsonify({'ok': True, 'filename': filename, 'rows': len(rows)})


if __name__ == '__main__':
    app.run(debug=True, port=5000)
