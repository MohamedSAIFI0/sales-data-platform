"""
Flask API — Plateforme ML Ventes Cassandra
==========================================
Routes :
  GET  /                     → Dashboard UI
  POST /api/train            → Entraîne les 3 modèles depuis Cassandra
  GET  /api/stats            → Stats temps réel depuis Cassandra
  POST /api/predict/status   → Prédit le statut d'une vente
  POST /api/predict/amount   → Prédit le montant TTC
  POST /api/predict/return   → Évalue le risque de retour
  GET  /api/models/info      → Infos sur les modèles sauvegardés
"""
import os
import json
import logging
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# ── Imports locaux ──
from ml_pipeline import (
    train_all, predict_status, predict_amount, predict_return_risk,
    CANAL_MAP, SEGMENT_MAP, REGION_MAP, MODEL_DIR,
)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)

# Cache léger pour éviter de se reconnecter à chaque requête
_session = None
_cluster = None
_last_train_results = {}


def get_cassandra():
    """Retourne (session, cluster) — réutilise la connexion existante."""
    global _session, _cluster
    if _session is None:
        from cassandra_connector import get_session
        _session, _cluster = get_session()
    return _session


def cassandra_available():
    try:
        get_cassandra()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────
# DONNÉES DEMO (si Cassandra non dispo)
# ─────────────────────────────────────────────
import pandas as pd
import numpy as np

def get_demo_data():
    """Génère des données synthétiques reproduisant le schéma Cassandra."""
    np.random.seed(42)
    n = 200
    canaux   = ["mobile", "magasin", "web", "marketplace"]
    statuts  = ["completee", "annulee", "en_attente"]
    regions  = list(REGION_MAP.keys())
    segments = list(SEGMENT_MAP.keys())

    ventes = pd.DataFrame({
        "vente_id":      range(1, n+1),
        "client_id":     np.random.randint(1, 51, n),
        "produit_id":    np.random.randint(1, 31, n),
        "canal":         np.random.choice(canaux, n),
        "region":        np.random.choice(regions, n),
        "statut":        np.random.choice(statuts, n, p=[0.55, 0.25, 0.20]),
        "prix_unitaire": np.random.uniform(50, 3000, n).round(2),
        "quantite":      np.random.randint(1, 11, n),
        "remise":        np.random.uniform(0, 0.25, n).round(2),
        "tva":           np.random.uniform(50, 500, n).round(2),
        "montant_ht":    np.random.uniform(100, 25000, n).round(2),
        "montant_ttc":   np.random.uniform(120, 30000, n).round(2),
        "date_vente":    pd.date_range("2023-01-01", periods=n, freq="2D"),
        "reference":    [f"VTE-{i:07d}" for i in range(n)],
    })

    clients = pd.DataFrame({
        "client_id": range(1, 51),
        "segment":   np.random.choice(segments, 50),
    })

    retours = pd.DataFrame({
        "retour_id":     range(1, 101),
        "client_id":     np.random.randint(1, 51, 100),
        "produit_id":    np.random.randint(1, 31, 100),
        "commande_ref":  [f"VTE-{np.random.randint(0,200):07d}" for _ in range(100)],
        "motif":         np.random.choice(["Produit défectueux","Livraison tardive","Mauvaise taille"], 100),
        "statut":        np.random.choice(["Accepté","Refusé","En cours"], 100),
        "montant_rembourse": np.random.uniform(50, 2000, 100).round(2),
    })

    return {"ventes": ventes, "clients": clients, "produits": pd.DataFrame(),
            "canaux": pd.DataFrame(), "retours": retours, "stocks": pd.DataFrame()}


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def health():
    return jsonify({
        "status": "ok",
        "cassandra": cassandra_available(),
        "timestamp": datetime.now().isoformat(),
    })


@app.route("/api/train", methods=["POST"])
def train():
    """Entraîne les 3 modèles ML depuis Cassandra (ou démo)."""
    global _last_train_results
    try:
        if cassandra_available():
            from cassandra_connector import load_all_data
            data = load_all_data(get_cassandra())
            source = "cassandra"
        else:
            data   = get_demo_data()
            source = "demo"

        results = train_all(data)
        _last_train_results = results

        return jsonify({
            "success": True,
            "source": source,
            "models_trained": list(results.keys()),
            "metrics": results,
            "timestamp": datetime.now().isoformat(),
        })
    except Exception as e:
        logger.exception("Erreur lors de l'entraînement")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stats")
def stats():
    """Stats agrégées depuis Cassandra (ou démo)."""
    try:
        if cassandra_available():
            from cassandra_connector import load_ventes, load_retours
            session = get_cassandra()
            ventes  = load_ventes(session)
            retours = load_retours(session)
        else:
            data    = get_demo_data()
            ventes  = data["ventes"]
            retours = data["retours"]

        total_ventes  = len(ventes)
        ca_total      = round(float(ventes["montant_ttc"].sum()), 2) if "montant_ttc" in ventes.columns else 0
        taux_complete = round((ventes["statut"] == "completee").mean() * 100, 1) if "statut" in ventes.columns else 0
        nb_retours    = len(retours)

        par_canal = {}
        if "canal" in ventes.columns:
            par_canal = ventes.groupby("canal")["montant_ttc"].sum().round(2).to_dict()

        par_statut = {}
        if "statut" in ventes.columns:
            par_statut = ventes["statut"].value_counts().to_dict()

        top_produits = {}
        if "produit_id" in ventes.columns and "montant_ttc" in ventes.columns:
            top_produits = (
                ventes.groupby("produit_id")["montant_ttc"]
                .sum().nlargest(5).round(2).to_dict()
            )

        return jsonify({
            "total_ventes":  total_ventes,
            "ca_total":      ca_total,
            "taux_completion": taux_complete,
            "nb_retours":    nb_retours,
            "par_canal":     par_canal,
            "par_statut":    par_statut,
            "top_produits":  {str(k): v for k, v in top_produits.items()},
            "source":        "cassandra" if cassandra_available() else "demo",
        })
    except Exception as e:
        logger.exception("Erreur stats")
        return jsonify({"error": str(e)}), 500


@app.route("/api/predict/status", methods=["POST"])
def api_predict_status():
    payload = request.get_json(force=True)
    try:
        result = predict_status(payload)
        return jsonify({"success": True, **result})
    except FileNotFoundError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Erreur predict_status")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/predict/amount", methods=["POST"])
def api_predict_amount():
    payload = request.get_json(force=True)
    try:
        result = predict_amount(payload)
        return jsonify({"success": True, **result})
    except FileNotFoundError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Erreur predict_amount")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/predict/return", methods=["POST"])
def api_predict_return():
    payload = request.get_json(force=True)
    try:
        result = predict_return_risk(payload)
        return jsonify({"success": True, **result})
    except FileNotFoundError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.exception("Erreur predict_return")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/models/info")
def models_info():
    import os, time
    info = {}
    for name in ["status", "amount", "return"]:
        path = os.path.join(MODEL_DIR, f"{name}_model.pkl")
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            info[name] = {
                "disponible": True,
                "derniere_maj": datetime.fromtimestamp(mtime).isoformat(),
                "taille_kb": round(os.path.getsize(path) / 1024, 1),
            }
        else:
            info[name] = {"disponible": False}
    return jsonify({"models": info, "last_metrics": _last_train_results})


# ─────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)