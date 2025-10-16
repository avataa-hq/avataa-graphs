from enum import Enum, StrEnum


class Status(StrEnum):
    NEW = "New"
    IN_PROCESS = "In Process"
    COMPLETE = "Complete"
    ERROR = "Error"


class LinkType(str, Enum):
    P_ID = "p_id"
    MO_LINK = "mo_link"
    TWO_WAY_MO_LINK = "two-way link"
    POINT_CONSTRAINT = "point_tmo_constraint"


class ConnectionType(str, Enum):
    P_ID = "p_id"
    MO_LINK = "mo_link"
    TWO_WAY_MO_LINK = "two-way link"
    POINT_A = "point_a"
    POINT_B = "point_b"
    COLLAPSED = "collapsed"
    GEOMETRY_LINE = "geometry_line"


class TrackingType(Enum):
    FULL = "Full"
    LOCAL = "Local"
    NONE = "None"
    GRAPH = "Graph"
    STRAIGHT = "Straight"
