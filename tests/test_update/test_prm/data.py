from task.models.incoming_data import MO, PRM


class TestPrmData:
    def __init__(self, data: list[dict]):
        self.__init_all(data=data)

    def __init_all(self, data: list[dict]):
        data_dict = {}
        for node in data:
            if node["data"]:
                data_dict[node["data"]["id"]] = node
        self._not_existing = self._get_not_existing(data_dict=data_dict)
        self._existing = self._get_existing(data_dict=data_dict)
        self._existing_mo_link = self._get_existing_mo_link(data_dict=data_dict)
        self._group = self._get_group(data_dict=data_dict)

    @staticmethod
    def _get_not_existing(data_dict: dict[int, dict]) -> PRM:
        real_mo_id = 11237479
        real_prm_id = 320958603
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        prm = [i for i in node.params if i.id == real_prm_id]
        result = prm[0]
        result.id = 10500
        result.tprm_id = 100500
        return result

    @staticmethod
    def _get_existing(data_dict: dict[int, dict]) -> PRM:
        real_mo_id = 11237479
        real_prm_id = 320958555
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        prm = [i for i in node.params if i.id == real_prm_id]
        result = prm[0]
        return result

    @staticmethod
    def _get_existing_mo_link(data_dict: dict[int, dict]) -> PRM:
        real_mo_id = 11237479
        real_prm_id = 320958603
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        prm = [i for i in node.params if i.id == real_prm_id]
        result = prm[0]
        return result

    @staticmethod
    def _get_group(data_dict: dict[int, dict]) -> PRM:
        real_mo_id = 11218705
        real_prm_id = 320152059
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        prm = [i for i in node.params if i.id == real_prm_id]
        result = prm[0]
        return result

    @property
    def not_existing(self) -> PRM:
        return self._not_existing.model_copy(deep=True)

    @property
    def existing(self) -> PRM:
        return self._existing.model_copy(deep=True)

    @property
    def existing_mo_link(self) -> PRM:
        return self._existing_mo_link.model_copy(deep=True)

    @property
    def group(self) -> PRM:
        return self._group.model_copy(deep=True)
