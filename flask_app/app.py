import os
import sys
import sqlite3
import subprocess
from flask import Flask, render_template, request, redirect, url_for, flash, send_file

app = Flask(__name__)
app.secret_key = 'fraud-detection-secret-2024'

# SQLite DB path – stored in the project's data/ folder
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'fraud.db'))


# ── Database helpers ──────────────────────────────────────────────────────────

def get_db():
    """Open a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # lets us access columns by name
    return conn


def init_db():
    """Create the suspicious_trades table if it doesn't already exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS suspicious_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_id       TEXT,
            stock           TEXT,
            quantity        INTEGER,
            reason          TEXT,
            suspicion_score INTEGER,
            detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print(f"[DB] SQLite database ready at: {DB_PATH}")


init_db()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    """Main dashboard – shows suspicious trades from SQLite."""
    results = []
    stats = {'total': 0, 'critical': 0, 'moderate': 0}
    try:
        conn = get_db()
        cursor = conn.execute(
            "SELECT * FROM suspicious_trades ORDER BY suspicion_score DESC"
        )
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        stats['total']    = len(results)
        stats['critical'] = sum(1 for r in results if r['suspicion_score'] >= 90)
        stats['moderate'] = sum(1 for r in results if 70 <= r['suspicion_score'] < 90)
    except Exception as e:
        flash(f'Database error: {e}', 'danger')

    return render_template('index.html', results=results, stats=stats)


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle CSV upload and trigger the full pipeline."""
    if 'orders' not in request.files:
        flash('No file part in the request.', 'warning')
        return redirect(url_for('index'))

    file = request.files['orders']
    if file.filename == '':
        flash('No file selected.', 'warning')
        return redirect(url_for('index'))

    if file and file.filename.lower().endswith('.csv'):
        # Save uploaded file to a temp path
        import tempfile, shutil
        tmp_dir  = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, 'orders.csv')
        file.save(tmp_path)

        # Use the local Python pipeline (works on Windows without bash/Hadoop)
        local_pipeline = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', 'scripts', 'local_pipeline.py')
        )

        try:
            result = subprocess.run(
                [sys.executable, local_pipeline, tmp_path, DB_PATH],
                capture_output=True, text=True, timeout=120,
                env={**os.environ, 'PYTHONIOENCODING': 'utf-8'}
            )
            if result.returncode == 0:
                flash('Pipeline completed successfully! Results updated below.', 'success')
            else:
                err = (result.stderr or result.stdout)[-500:]
                flash(f'Pipeline failed: {err}', 'danger')
        except subprocess.TimeoutExpired:
            flash('⏱️ Pipeline timed out (>2 min). Check your data file.', 'danger')
        except Exception as e:
            flash(f'❌ Pipeline error: {e}', 'danger')
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        return redirect(url_for('index'))
    else:
        flash('Only .csv files are accepted.', 'danger')
        return redirect(url_for('index'))


@app.route('/clear', methods=['POST'])
def clear_results():
    """Clear all suspicious trade records from SQLite."""
    try:
        conn = get_db()
        conn.execute("DELETE FROM suspicious_trades")
        conn.commit()
        conn.close()
        flash('All records cleared.', 'info')
    except Exception as e:
        flash(f'Error clearing records: {e}', 'danger')
    return redirect(url_for('index'))


@app.route('/sample')
def download_sample():
    """Serve the bundled sample orders.csv for download."""
    sample_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'data', 'orders.csv')
    )
    return send_file(sample_path, as_attachment=True, download_name='sample_orders.csv')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
