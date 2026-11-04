"""Versioned canonical field mapping."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pncp_etl.pncp.errors import ContractError

MAPPING_VERSION = "2"


class FieldKind(StrEnum):
    """Supported JSON field families."""

    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    CNPJ = "cnpj"


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """One explicit source-to-canonical mapping rule."""

    source: str
    target: str
    kind: FieldKind
    required: bool = False


FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec("numeroControlePNCPAta", "numero_controle_pncp_ata", FieldKind.STRING, True),
    FieldSpec("numeroAtaRegistroPreco", "numero_ata_registro_preco", FieldKind.STRING),
    FieldSpec("anoAta", "ano_ata", FieldKind.INTEGER),
    FieldSpec("numeroControlePNCPCompra", "numero_controle_pncp_compra", FieldKind.STRING),
    FieldSpec("cancelado", "cancelado", FieldKind.BOOLEAN),
    FieldSpec("dataCancelamento", "data_cancelamento", FieldKind.DATETIME),
    FieldSpec("dataAssinatura", "data_assinatura", FieldKind.DATETIME),
    FieldSpec("vigenciaInicio", "vigencia_inicio", FieldKind.DATETIME),
    FieldSpec("vigenciaFim", "vigencia_fim", FieldKind.DATETIME),
    FieldSpec("dataPublicacaoPncp", "data_publicacao_pncp", FieldKind.DATETIME),
    FieldSpec("dataInclusao", "data_inclusao", FieldKind.DATETIME),
    FieldSpec("dataAtualizacao", "data_atualizacao", FieldKind.DATETIME),
    FieldSpec("dataAtualizacaoGlobal", "data_atualizacao_global", FieldKind.DATETIME),
    FieldSpec("usuario", "usuario", FieldKind.STRING),
    FieldSpec("objetoContratacao", "objeto_contratacao", FieldKind.STRING),
    FieldSpec("cnpjOrgao", "cnpj_orgao", FieldKind.CNPJ),
    FieldSpec("nomeOrgao", "nome_orgao", FieldKind.STRING),
    FieldSpec("codigoUnidadeOrgao", "codigo_unidade_orgao", FieldKind.STRING),
    FieldSpec("nomeUnidadeOrgao", "nome_unidade_orgao", FieldKind.STRING),
    FieldSpec("cnpjOrgaoSubrogado", "cnpj_orgao_subrogado", FieldKind.CNPJ),
    FieldSpec("nomeOrgaoSubrogado", "nome_orgao_subrogado", FieldKind.STRING),
    FieldSpec(
        "codigoUnidadeOrgaoSubrogado",
        "codigo_unidade_orgao_subrogado",
        FieldKind.STRING,
    ),
    FieldSpec("nomeUnidadeOrgaoSubrogado", "nome_unidade_orgao_subrogado", FieldKind.STRING),
    FieldSpec("possibilidadeAdesao", "possibilidade_adesao", FieldKind.BOOLEAN),
)


def validate_mapping(specs: tuple[FieldSpec, ...]) -> None:
    """Reject source or target collisions before processing records."""
    sources: set[str] = set()
    targets: set[str] = set()
    for spec in specs:
        if not spec.source or not spec.target:
            raise ContractError("mapping source and target must not be empty")
        if spec.source in sources:
            raise ContractError(f"duplicate mapping source: {spec.source}")
        if spec.target in targets:
            raise ContractError(f"duplicate mapping target: {spec.target}")
        sources.add(spec.source)
        targets.add(spec.target)


validate_mapping(FIELD_SPECS)
