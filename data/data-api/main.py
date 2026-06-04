from fastapi import FastAPI
from datetime import date, timedelta
import random, time

app = FastAPI(title="Sales Data API")

CANAUX  = ["magasin", "web", "mobile", "marketplace"]
REGIONS = ["Casablanca-Settat", "Rabat-Sale-Kenitra", "Fes-Meknes",
           "Marrakech-Safi", "Souss-Massa", "Tanger-Tetouan"]
STATUTS = ["completee", "completee", "completee", "annulee", "en_attente"]
DEPOTS  = ["DEP-CASA-01", "DEP-CASA-02", "DEP-RBAT-01", "DEP-FES-01", "DEP-MRCH-01"]

def rand_date():
    start = date(2023, 1, 1)
    return (start + timedelta(days=random.randint(0, 729))).isoformat()

def make_vente(i):
    prix   = round(random.uniform(50, 3000), 2)
    qte    = random.randint(1, 10)
    remise = round(random.uniform(0, 0.25), 2)
    ht     = round(prix * qte * (1 - remise), 2)
    tva    = round(ht * 0.20, 2)
    return {
        "vente_id":      i,
        "reference":     f"VTE-{int(time.time())}-{i:04d}",
        "date_vente":    rand_date(),
        "client_id":     random.randint(1, 50),
        "produit_id":    random.randint(1, 30),
        "quantite":      qte,
        "prix_unitaire": prix,
        "remise":        remise,
        "montant_ht":    ht,
        "tva":           tva,
        "montant_ttc":   round(ht + tva, 2),
        "canal":         random.choice(CANAUX),
        "region":        random.choice(REGIONS),
        "statut":        random.choice(STATUTS)
    }

def make_stock(i):
    dispo = random.randint(0, 300)
    return {
        "stock_id":            i,
        "produit_id":          random.randint(1, 30),
        "code_produit":        f"PROD-{random.randint(1, 30):05d}",
        "depot":               random.choice(DEPOTS),
        "quantite_disponible": dispo,
        "quantite_reservee":   random.randint(0, min(30, dispo + 1)),
        "seuil_reappro":       random.randint(10, 50),
        "statut":              "OK" if dispo > 10 else ("FAIBLE" if dispo > 0 else "RUPTURE"),
        "timestamp":           int(time.time())
    }

@app.get("/")
def root():
    return {"service": "Sales Data API", "endpoints": ["/ventes", "/stocks", "/health"]}

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": int(time.time())}

@app.get("/ventes")
def get_ventes(batch_size: int = 10):
    return {
        "timestamp": int(time.time()),
        "batch_size": batch_size,
        "data": [make_vente(i) for i in range(1, batch_size + 1)]
    }

@app.get("/stocks")
def get_stocks(batch_size: int = 5):
    return {
        "timestamp": int(time.time()),
        "batch_size": batch_size,
        "data": [make_stock(i) for i in range(1, batch_size + 1)]
    }
