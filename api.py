"""
REST API for SNCR rural property data.

GET /imovel/{codigo_incra} - Returns property data with owners.
"""

import os
import re

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException

DB_DSN = os.getenv("DATABASE_URL", "postgresql://sncr:sncr@localhost:5432/sncr")

app = FastAPI(title="SNCR API", description="Rural property lookup")


def get_db():
    return psycopg2.connect(DB_DSN)


def anonymize_cpf(cpf: str) -> str:
    """
    Anonymizes CPF showing only the last 2 digits before the check digit.

    Source CPFs come partially masked: ***.200.221-**
    A full CPF is: AAA.BBB.CCC-VV (9 main digits + 2 check digits)
    The last 2 digits before check digit = positions 8 and 9 (last 2 of CCC).

    Example: '***.200.221-**' -> '***.***.***-21'
    """
    digits = re.findall(r"\d", cpf)

    if len(digits) < 2:
        return "***.***.***-**"

    return f"***.***.***-{digits[-2]}{digits[-1]}"


@app.get("/imoveis")
def list_properties():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT incra_code, name, municipality, state FROM properties ORDER BY state, municipality"
            )
            rows = cur.fetchall()
        return [
            {
                "codigo_incra": r["incra_code"],
                "denominacao": r["name"],
                "municipio": r["municipality"],
                "uf": r["state"],
                "link": f"/imovel/{r['incra_code']}",
            }
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/imovel/{incra_code}")
def get_property(incra_code: str):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT incra_code, area_hectares, status, name FROM properties WHERE incra_code = %s",
                (incra_code,),
            )
            prop = cur.fetchone()

            if not prop:
                raise HTTPException(
                    status_code=404,
                    detail=f"Property with INCRA code '{incra_code}' not found.",
                )

            cur.execute(
                """SELECT owner_name, cpf, relationship, participation_pct
                   FROM owners
                   WHERE incra_code = %s
                   ORDER BY participation_pct DESC""",
                (incra_code,),
            )
            owners = cur.fetchall()

        return {
            "codigo_incra": prop["incra_code"],
            "denominacao": prop["name"],
            "area_ha": float(prop["area_hectares"]) if prop["area_hectares"] else None,
            "situacao": prop["status"],
            "proprietarios": [
                {
                    "nome_completo": o["owner_name"],
                    "cpf": anonymize_cpf(o["cpf"]),
                    "vinculo": o["relationship"],
                    "participacao_pct": float(o["participation_pct"]) if o["participation_pct"] else None,
                }
                for o in owners
            ],
        }
    finally:
        conn.close()
