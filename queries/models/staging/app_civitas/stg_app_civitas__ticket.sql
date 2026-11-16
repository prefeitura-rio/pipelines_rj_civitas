{{
    config(
        alias = 'ticket'
    )
}}

WITH ticket AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'tickets')) }}
)

SELECT
    id AS id_ticket,
    internal_number AS numero_interno,
    parent_ticket_id AS id_ticket_pai,
    operation_id AS id_operacao,
    ticket_type_id AS id_ticket_tipo,
    nature_id AS id_ticket_natureza,
    team_id AS id_equipe,
    finalized_by_team_id AS id_equipe_finalizadora,
    {{ codigo_para_texto('status') }} AS status,
    {{ codigo_para_texto('priority') }} AS prioridade,
    procedure_number AS numero_procedimento,
    official_letter_number AS numero_oficio,
    sei_process_number AS numero_processo_sei,
    sei_process_link AS link_processo_sei,
    base_date AS base_data,
    has_press_nickname AS apelido_imprensa_indicador,
    press_nickname AS apelido_imprensa,
    press_link AS link_imprensa,
    has_correspondence_address AS correspondencia_endereco_indicador,
    correspondence_street AS correspondencia_logradouro,
    correspondence_number AS correspondencia_numero,
    correspondence_neighborhood AS correspondencia_bairro,
    requester_name AS solicitante_nome,
    requester_email AS solicitante_email,
    requester_phone AS solicitante_telefone,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    operational_closed_at AS encerramento_operacional_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket
