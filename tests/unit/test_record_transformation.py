"""Tests for record validation and canonical mapping."""

from copy import deepcopy

import pytest

from pncp_etl.pncp.errors import ContractError
from pncp_etl.quality.mapping import FIELD_SPECS, FieldKind, FieldSpec, validate_mapping
from pncp_etl.quality.records import validate_and_transform


def test_valid_record_is_normalized_without_mutating_raw() -> None:
    """Mapped values are canonical while the source dictionary remains untouched."""
    payload = {
        "numeroControlePNCPAta": " ATA-1 ",
        "numeroAtaRegistroPreco": " 42 ",
        "anoAta": 2026,
        "cancelado": False,
        "vigenciaInicio": "2026-11-02T09:00:00-03:00",
        "vigenciaFim": "2026-11-03T09:00:00-03:00",
        "dataAtualizacaoGlobal": "2026-11-02T12:00:00Z",
        "cnpjOrgao": "12.345.678/0001-95",
        "codigoUnidadeOrgao": "001",
        "campoNovo": "fica somente no raw",
    }
    original = deepcopy(payload)

    outcome = validate_and_transform(payload)

    assert outcome.issues == ()
    assert outcome.canonical is not None
    assert outcome.canonical["numero_controle_pncp_ata"] == "ATA-1"
    assert outcome.canonical["numero_ata_registro_preco"] == "42"
    assert outcome.canonical["cancelado"] is False
    assert outcome.canonical["cnpj_orgao"] == "12345678000195"
    assert outcome.canonical["codigo_unidade_orgao"] == "001"
    assert outcome.canonical["vigencia_inicio_local"] == "2026-11-02T09:00:00"
    assert outcome.canonical["vigencia_inicio_utc"] == "2026-11-02T12:00:00Z"
    assert "campo_novo" not in outcome.canonical
    assert payload == original


def test_optional_absent_and_null_fields_remain_null() -> None:
    """Missing optional values are explicit nulls, never empty strings."""
    outcome = validate_and_transform(
        {"numeroControlePNCPAta": "ATA-1", "objetoContratacao": None}
    )

    assert outcome.issues == ()
    assert outcome.canonical is not None
    assert outcome.canonical["objeto_contratacao"] is None
    assert outcome.canonical["ano_ata"] is None
    assert outcome.canonical["data_atualizacao_global_local"] is None
    assert outcome.canonical["data_atualizacao_global_utc"] is None


def test_real_pncp_date_only_fields_are_local_without_invented_utc() -> None:
    """Date-only source values observed in PNCP are valid local projections."""
    outcome = validate_and_transform(
        {
            "numeroControlePNCPAta": "00508903000188-1-001937/2025-000001",
            "dataAssinatura": "2025-09-19",
            "vigenciaInicio": "2025-09-23",
            "vigenciaFim": "2027-09-23",
            "dataInclusao": "2025-09-22",
            "dataAtualizacao": "2026-09-24",
            "dataAtualizacaoGlobal": "2026-11-02",
            "cnpjOrgao": "00508903000188",
        }
    )

    assert outcome.issues == ()
    assert outcome.canonical is not None
    assert outcome.canonical["data_atualizacao_global_local"] == "2026-11-02T00:00:00"
    assert outcome.canonical["data_atualizacao_global_utc"] is None


@pytest.mark.parametrize(
    ("payload", "reason_code"),
    [
        ({}, "missing_business_key"),
        ({"numeroControlePNCPAta": "   "}, "missing_business_key"),
        ({"numeroControlePNCPAta": "A", "cancelado": 1}, "invalid_type"),
        ({"numeroControlePNCPAta": "A", "anoAta": True}, "invalid_type"),
        ({"numeroControlePNCPAta": "A", "cnpjOrgao": "123"}, "invalid_cnpj"),
        (
            {"numeroControlePNCPAta": "A", "dataAssinatura": "2026-11-02 09:30:15"},
            "invalid_datetime",
        ),
        (
            {
                "numeroControlePNCPAta": "A",
                "dataAssinatura": "2026-11-02T09:30:15.1234567Z",
            },
            "unsupported_datetime_precision",
        ),
    ],
)
def test_invalid_record_is_quarantined_with_stable_reason(
    payload: dict[str, object], reason_code: str
) -> None:
    """Invalid values never produce a partially canonical record."""
    outcome = validate_and_transform(payload)

    assert outcome.canonical is None
    assert reason_code in {issue.reason_code for issue in outcome.issues}


def test_mixed_validity_domains_are_not_compared() -> None:
    """A local start and offset-aware end cannot prove a valid interval."""
    outcome = validate_and_transform(
        {
            "numeroControlePNCPAta": "A",
            "vigenciaInicio": "2026-11-02T09:00:00",
            "vigenciaFim": "2026-11-03T09:00:00Z",
        }
    )

    assert outcome.canonical is None
    assert {issue.reason_code for issue in outcome.issues} == {
        "incomparable_temporal_domain"
    }


def test_inverted_validity_range_is_rejected() -> None:
    """Comparable validity endpoints must be ordered."""
    outcome = validate_and_transform(
        {
            "numeroControlePNCPAta": "A",
            "vigenciaInicio": "2026-11-03T09:00:00Z",
            "vigenciaFim": "2026-11-02T09:00:00Z",
        }
    )

    assert outcome.canonical is None
    assert {issue.reason_code for issue in outcome.issues} == {"invalid_validity_range"}


def test_mapping_collision_is_a_structural_error() -> None:
    """Two sources cannot silently overwrite the same canonical destination."""
    duplicate = FieldSpec(
        source="anotherSource",
        target=FIELD_SPECS[0].target,
        kind=FieldKind.STRING,
        required=False,
    )

    with pytest.raises(ContractError, match="target"):
        validate_mapping((*FIELD_SPECS, duplicate))
