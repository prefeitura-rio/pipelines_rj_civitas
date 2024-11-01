# -*- coding: utf-8 -*-
"""
Imports all flows for every project so we can register all of them................
"""
from pipelines.alertas_discord import *  # noqa
from pipelines.disque_denuncia import *  # noqa
from pipelines.fogo_cruzado import *  # noqa
from pipelines.integracao_reports import *  # noqa
from pipelines.integracao_reports_staging import *  # noqa
from pipelines.radar_readings import *  # noqa
from pipelines.radares_infra import *  # noqa
from pipelines.scraping_redes import *  # noqa
from pipelines.templates import *  # noqa
from pipelines.update_sheets import *  # noqa
