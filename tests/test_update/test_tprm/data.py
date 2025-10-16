from dateutil.parser import isoparse

from task.models.incoming_data import TPRM

not_existing_tprm = TPRM(
    name="not_existing_tprm",
    val_type="mo_link",
    required=False,
    returnable=True,
    id=1,
    tmo_id=44481,
    multiple=False,
    description="",
    constraint="[44484]",
    prm_link_filter=None,
    group=None,
    created_by=None,
    modified_by=None,
    creation_date=None,
    modification_date=None,
    version=1,
    field_value=None,
)

group_tprm = TPRM(
    name="Region",
    val_type="mo_link",
    required=True,
    tmo_id=43622,
    id=130066,
    constraint="[43623]",
    field_value="",
    created_by="",
    modified_by="",
    creation_date=isoparse("2024-03-05T15:55:06.010526Z"),
    modification_date=isoparse("2024-03-05T15:55:06.010529Z"),
    version=1,
    multiple=False,
    returnable=False,
)

trace_tprm = TPRM(
    name="Service Name",
    val_type="formula",
    required=False,
    tmo_id=43626,
    id=131818,
    constraint="if parameter['Service Type'] == '010' then parameter['Service ID'].split()[0].split('-')[2] + "
    "'-' + parameter['Service ID'].split()[2].split('-')[2] + ' RING XH ' + "
    "parameter['Service ID'].split()[1][-2:]; elif parameter['Service Type'] == '020' then '3G ' + "
    "parameter['Service ID'].split()[0].split('-')[2] + ' ' + parameter['Service ID'].split()[1][-2:]; "
    "elif parameter['Service Type'] == '133' then 'WoW ' + "
    "parameter['Service ID'].split()[0].split('-')[2] + ' ' + parameter['Service ID'].split()[1][-2:]; "
    "elif parameter['Service Type'] == '030' then 'FIBER FTTx ' + "
    "parameter['Service ID'].split()[0].split('-')[1] + '-' + "
    "parameter['Service ID'].split()[0].split('-')[2] + ' ' + "
    "parameter['Service ID'].split()[2].split('-')[2]; else parameter['Service ID']",
    field_value="",
    created_by="",
    modified_by="",
    creation_date=isoparse("2024-05-10T13:00:39.840681Z"),
    modification_date=isoparse("2024-05-16T17:55:27.942932Z"),
    version=1,
    multiple=False,
    returnable=False,
)
