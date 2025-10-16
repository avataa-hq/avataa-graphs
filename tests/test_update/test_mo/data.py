from task.models.incoming_data import MO


class TestMoData:
    def __init__(self, data: list[dict]):
        self.__init_all(data=data)

    def __init_all(self, data: list[dict]):
        data_dict = {}
        for node in data:
            if node["data"]:
                data_dict[node["data"]["id"]] = node
        self._not_existing = self._get_not_existing(data_dict=data_dict)
        self._top_level_node = self._get_top_level_node(data_dict=data_dict)
        self._grouping_node = self._get_grouping_node(data_dict=data_dict)

    @staticmethod
    def _get_not_existing(data_dict: dict[int, dict]) -> MO:
        real_mo_id = 11237479
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        result = MO(
            tmo_id=node.tmo_id,
            p_id=node.p_id,
            id=100500,
            name="Test not existing",
            active=True,
            version=1,
            point_a_id=node.point_a_id,
            point_b_id=node.point_b_id,
        )
        return result

    @staticmethod
    def _get_top_level_node(data_dict: dict[int, dict]) -> MO:
        real_mo_id = 11218703
        node = MO.model_validate(data_dict[real_mo_id]["data"])
        result = node
        return result

    @staticmethod
    def _get_grouping_node(data_dict: dict[int, dict]) -> MO:
        real_mo_id = 11218703
        result = MO.model_validate(data_dict[real_mo_id]["data"])
        result.id = 100500
        result.name = "Test"
        for param in result.params:  # type: PRM
            if param.id != 320152054:
                continue
            param.value = 100500
            break
        return result

    @property
    def not_existing(self) -> MO:
        return self._not_existing.model_copy(deep=True)

    @property
    def top_level_node(self):
        return self._top_level_node

    @property
    def grouping_node(self):
        return self._grouping_node
