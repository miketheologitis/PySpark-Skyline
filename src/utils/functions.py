from configurations import MAXIMUM_VALUE, MINIMUM_VALUE
from pyspark.sql.types import IntegerType, StructField, StructType, ArrayType
from math import atan, pi, sqrt


def is_dominated(better_functions, point, other_point):
    """ Using the 'better_functions' representing the Skyline query finds whether or not 'point' is dominated by 'other_point'

    Keyword arguments:
    :param better_functions : A list of length equal to the dimension 'd' of the points that has 'd' Python functions 
        (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a point is better in the
        i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    :param point : The point (tuple)
    :param other_point : The other point (tuple)

    :returns The True if 'point' is dominated by 'other_point', False otherwise
    """
    potential_domination = False
    for func, p, op in zip(better_functions, point, other_point):
        if func(p, op) == p and p != op:
            # then point is better at this coordinate than other_point, thus not dominated
            return False
        if func(p, op) == op and p != op:
            # then other_point is better at this coordinate than point, there could be domination
            potential_domination = True

    return potential_domination

def find_skyline(better_functions, points):
    """ Using the 'better_functions' representing the Skyline query finds the skyline of the 'points'

    Keyword arguments:
    :param better_functions : A list of length equal to the dimension 'd' of the points that has 'd' functions 
        (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a  point is better
        in the i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    :param points : List of points (tuples)

    :returns The skyline as a list of points (tuples)
    """
    skyline = set()
    remove_from_skyline = set()
    
    for point in points:
        for other_point in skyline:
            if is_dominated(better_functions, point, other_point):
                break
            if is_dominated(better_functions, other_point, point):
                remove_from_skyline.add(other_point)
        else:
            # no other point dominates point in skyline. inner for-loop exhausted
            skyline.add(point)
        skyline.difference_update(remove_from_skyline) # remove dominated points
        remove_from_skyline.clear() #  clear the set
    return skyline


def mr_dim_partition_key(number_of_partitions, x):
    """ Using the MR-Dim partitioning scheme this function gives the partitioning 'key' of a point based on how
    many TOTAL partitions we want and the value of some fixed dimension (for the whole application) x_i.

    Keyword arguments:
    :param number_of_partitions: The total number of partitions in the MR-Dim Scheme. As the paper suggested,
        empirically it is equal to 2 * (number of nodes). (i.e. Query was "x_i MIN")
    :param x: The value of the point we are examining at some fixed dimension i (in this application we always use x1).

    :returns A key = 0, ..., number_of_partitions-1 based on which partition (or else, disjoint subspace) it belongs to.
        We chose to do this with integer division (very straightforward).
    """
    if x == MAXIMUM_VALUE:
        return number_of_partitions-1

    partition_range = (MAXIMUM_VALUE - MINIMUM_VALUE) / number_of_partitions
    return int(x // partition_range)


def mr_grid_partition_key(b, *x):
    """ We will analyze the case where we cut each dimension in half (b=2) and we will generalize later. Now, 
    using the MR-Grid partitioning scheme this function gives the partitioning 'key' of a point. Notice that
    in MR-Grid we have 2^d partitions (b=2), where 'd' is the dimension. Every dimension gets cut in half and the intersection
    of those cut subspaces is a partition. Now, the 1st property that our partitioning 'key' must have is that it has the
    same value for any point belonging to a partition of space P and a different value if that point does not belong
    to P. We chose our 'key' = 0, ..., 2^d-1 (2nd property). Since we cut each dimension in half, a point either belongs to the first
    part of the i-th dimension (0 bit) or the second half (1 bit). Well, what could our 'key' be that satisfies the two
    properties? Well, it's straightforward to see that binary -> decimal satisfies our two 'key' properties, where our
    point p = (x_1, x_2, ...) is translated to a bit vector with LSB x_1 and MSB x_d.

    We give a few examples with MINIMUM_VALUE = 0, and MAXIMUM_VALUE = 10
    d=3 , (1, 6, 10) -> 011 -> 'key' = (2^0 * 0) + (2^1 * 1) + (2^2 * 1) = 6 
    d=3 , (6, 10, 7) -> 111 -> 'key' = (2^0 * 1) + (2^1 * 1) + (2^2 * 1) = 7
    d=3 , (6, 1, 1) -> 100 -> 'key' = (2^0 * 1) + (2^1 * 0) + (2^2 * 0) = 1
    d=5 , (3, 2, 6, 8, 1) -> 00110 -> ... -> 'key' = 12

    We will use the 'mr_dim_partition_key(2, x_i)' to get the i-th bit (0 or 1) for the following implementation.

    Now, as the paper mentions, there is a dominance relationship between partitions. Assuming we are in 2-dimensions
    and the query is 'SKYLINE OF x1 MIN, x2 MIN', there are 4 partitions and the bottom left partition dominates the
    up-right partition, as all points in the up-right partition are dominated by any point in the bottom-left partition.

    With this scheme, let's consider a query 'SKYLINE OF x1 MIN, ..., xd MIN'. We can view the dominance relationship
    between partitions with the bits. What I mean is that a partition with key 000...0 always dominates the partition
    with key 111...1 (in each dimension 0 bit is always better than 1 bit, in this query). There is only one certain domination, i.e.,
    000...0 dominates 111...1 . There is no other certain domination. Now, notice that in a query 'SKYLINE OF x1 MIN, x2 MAX, x3 MIN' 
    in the second dimension (x2) the better bit is 1 (1 bit is always better than 0). This means that again we have one certain
    domination, i.e., the partition with key 010 always dominates partition with key 101.

    One importand thing to notice (which will come in handy later) is that the best partition, for instance the onewith key = 000...0 
    when the query is 'SKYLINE OF x1 MIN, ..., xd MIN', always dominates any partition that is dominated by another partition. 
    (This requires some thought but it is indeed true). A simple schetch of the proof is the following: Let B be the best
    partition and let A partition dominate C partition. B is equivalent in every dimension to A (better in atleast one dimension). 
    Since A dominates C, A is better in all dimensions than C, and by definition of the best dimension, B dominates C aswell.

    VERY IMPORTAND: Corollary: A partition can be filtered out if and only if it is dominated by the best partition. 

    GENERALIZATION: Now, we give the option to cut each dimension into a variable amount of subspaces. We explained before
    the simple case where we cut each dimension into two halves. Now, when we cut each dimension into 'b' equal subspaces
    the only change we must do is to translate the point p = (x_1, x_2, ...) to 'b'-base (previously we translated it to binary 
    because b=2). Everything mentioned before holds.

    We give a few examples with MINIMUM_VALUE = 0, and MAXIMUM_VALUE = 10
    d=3 b=4, (1, 6, 10) -> 023 -> 'key' = (4^0 * 0) + (4^1 * 2) + (4^2 * 3) = 56
    d=3 b=4, (6, 10, 7) -> 232 -> 'key' = (4^0 * 2) + (4^1 * 3) + (4^2 * 2) = 46
    d=3 b=4, (6, 1, 1) -> 200 -> 'key' = (4^0 * 2) + (4^1 * 0) + (4^2 * 0) = 2
    d=5 b=4, (3, 2, 6, 8, 1) -> 10230 -> ... -> 'key' = 225

    Keyword arguments:
    :param b : In how many (equal) disjoint subspaces we cut each dimension in (simplest case being 2 -> two halves)
    :param x: Contains the coordinate value for each dimension of a point. Ex. x[0] -> x1_value, x[d-1] -> xd_value

    :returns A partitioning 'key' = 0, ..., b^d-1, following the MR-Grid partitioning scheme 
    """

    # Converting the point into 'b'-base as explained and getting the corresponding key.
    return sum([
        b**i * mr_dim_partition_key(b, x_i) for i, x_i in enumerate(x)
    ])


def best_mr_grid_partition_key(better_functions, b):
    """ Returns the key of the best partition in 'b'-base as a reversed list. This follows the logic of mr_grid_partition_key so make
    sure you read the comments. 
    For example:
        best_mr_grid_partition_key([min, min, min], 5) : [0, 0, 0]      (Query: 'SKYLINE OF x1 MIN, x2 MIN, x3 MIN')
        best_mr_grid_partition_key([min, min, max], 5) : [0, 0, 4]      (Query: 'SKYLINE OF x1 MIN, x2 MIN, x3 MAX')
        best_mr_grid_partition_key([max, max, max], 5) : [4, 4, 4]      (Query: 'SKYLINE OF x1 MAX, x2 MAX, x3 MAX')

    Keyword arguments:
    :param better_functions : A list of length equal to the dimension 'd' of the points that has 'd' functions 
        (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a  point is better
        in the i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    :param b : In how many (equal) disjoint subspaces we cut each dimension in (simplest case being 2 -> two halves)

    :returns The 'b'-base representation of the key of the best partition (in reverse order)
    """
    return [func(0, b-1) for func in better_functions]


def mr_grid_partition_dominates_partition(better_functions, b_base_key_1, b_base_key_2):
    """ Returns True if partition with key (in 'b'-base) b_base_key_1 dominates partition with key (in 'b'-base) b_base_key_2 

    Keyword arguments:
    :param better_functions : A list of length equal to the dimension 'd' of the points that has 'd' functions 
        (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a  point is better
        in the i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    :param b_base_key_1 : (list) The 'b'-base representation of the key of partition 1 in reversed order
    :param b_base_key_2 : (list) The 'b'-base representation of the key of partition 2 in reversed order

    :returns True if partition 1 dominates partition 2, False otherwise
    """
    dominates = True
    for func, key_1_i, key_2_i in zip(better_functions, b_base_key_1, b_base_key_2):
        dominates = dominates and (func(key_1_i, key_2_i) == key_1_i and key_1_i != key_2_i)
        if not dominates: 
            break
    return dominates


def mr_grid_keep_partition(better_functions, best_b_base_key, b_base_key):
    """ Returns True if we do not want to filter out tuple with partition key 'b_base_key', False otherwise. As mentioned in
    the comments of 'mr_grid_partition_key', to see if we can filter out completely a (tuple) partition, we must compare its key
    with the key of the best partition, i.e., 'best_b_base_key'

    Keyword arguments:
    :param better_functions : A list of length equal to the dimension 'd' of the points that has 'd' functions 
        (currently either 'min' or 'max'). If 'better_functions[i]' = min then we know that a  point is better
        in the i-th dimension than another point if its x_i value is smaller (i.e. Query was "x_i MIN")
    :param best_b_base_key : The 'b'-base representation of the key of the BEST partition
    :param b_base_key : The 'b'-base representation of the key of the examined partition

    :returns True if the BEST partition does NOT dominate the examined partition (Keep the examined partition),
        False otherwise (Filter out the examined partition)
    """
    return not mr_grid_partition_dominates_partition(better_functions, best_b_base_key, b_base_key)


def decimal_to_base(n, b, d):
    """ Converts a decimal number 'n' to the equivalent number in 'b'-base as a (reversed) list with exactly 'd' elements.

    For example (Notice the reversed order):
    decimal_to_base(3, 2, 3) : [1, 1, 0]
    decimal_to_base(1, 2, 3) : [1, 0, 0]
    decimal_to_base(3, 3, 3) : [0, 1, 0]
    decimal_to_base(25, 3, 5) : [1, 2, 2, 0, 0]
    decimal_to_base(0, 3, 5) : [0, 0, 0, 0, 0]
    decimal_to_base(11, 10, 5) : [1, 1, 0, 0, 0]

    Keyword arguments:
    :param n : A decimal number to convert
    :param b : The base to convert to
    :param d : The dimension of the points, and the number of returned digits in the 'b'-base representation of 'n'

    :returns A list in reversed order of the digits in 'b'-base of the number 'n'. We always return 'd' number of digits
        in the list.
    """
    if n == 0:
        return [0] * d
    digits = []
    while n:
        digits.append(int(n % b))
        n //= b
    return digits + ([0] * (d - len(digits)))


def mr_angle_partition_key(number_angular_partitions, *x):
    """ WE ARE UNDER THE ASSUMPTION THAT ALL points BELONG TO THE FIRST ORTHANT (i.e. positive coordinates) AND WE ANSWER MIN QUERIES
        IN ALL DIMENSIONS. TODO: Generalize

    Following the MR_ANGLE scheme we partition points based on the angular coordinates and since all points belong to the first orthant
    every angular coordinate is between 0 and 90 (both inclusive). Considering points belong to R^d we have d-1 angular coordinates.
    First, each angular coordinate is cut in 'number_of_angular_partitions' number of equal disjoint ranges 0-90. Fixing for every
    angular coordinate its range we get a single partition. Hence, the number of total partitions is number_angular_partitions^(d-1).
    All the combinations of ranges of angular coordinates gives us the first orthant. We follow the idea of 'mr_grid_partition_key'
    and return a key = 0, ..., number_of_angular_partitions^(d-1). Such a key is 'number_of_angular_partitions'-base to decimal
    conversion. This is exactly our choice. Maybe this explanation is not very clear, so we can look at it another way: We have
    (d-1) numbers taking values from 0, ..., 'number_of_angular_partions'-1 and we want a decimal key representing this vector.
    Well, the most obvious choice is the conversion from 'number_of_angular_partions'-base to decimal!

    Let's assume number_angular_partitions = 2 and d = 2 (we divide the first quadrant in 2 partitions based on the MR_ANGLE scheme).
    Then, the partitioning of the (single) angular phi has two keys, i.e., 0 and 1. When phi is between 0 and 45 we have key equal
    to 0 and when phi is between 45 and 90 we have key equal to 1. Notably, the reason we only have two partitions is because there
    is a single angular coordinate in two dimensional space.

    Let's now move to R^3 with number_angular_partitions = 2. Now, we have two angular coordinates (polar, azimuthal angles). Each 
    of those two angular coordinates will be partitioned in to two parts (0-45) and (45-90) and the total partitions of R^3
    will be 4. With some imagination you can see the reason why https://en.wikipedia.org/wiki/Spherical_coordinate_system 
    (first diagram to the right).

    Keyword arguments:
    :param number_angular_partitions : Number of 'partitions' of the angular coordinate phi_i. (solely the angular coordinate phi_i)
    :param x : Contains the coordinate value for each dimension of a point. Ex. x[0] -> x1_value, x[d-1] -> xd_value

    :returns The MR_ANGLE partitioning key.
    """
    return sum([
        number_angular_partitions**i * mr_angle_dimensional_key(number_angular_partitions, x_i, x[i+1:]) 
        for i, x_i in enumerate(x[:-1])
    ])



def mr_angle_dimensional_key(number_angular_partitions, v_i, v):
    """ WE ARE UNDER THE ASSUMPTION THAT ALL points BELONG TO THE FIRST ORTHANT (i.e. positive coordinates) AND WE ANSWER MIN QUERIES
        IN ALL DIMENSIONS. TODO: Generalize

    Let points be in R^d. In MR_ANGLE we can consider the angular coordinates (of the hypersphere) as vectors containing 
    angles. Each angular coordinate phi_i is computed by the arctan( || (v_d , ... , v_(i-1)) ||_2 / v_i ), i.e. inverse tan of 
    the l2 norm of the elements from d to i (exclusive) divided by v_i. Following the explanation in 'mr_angle_partition_key'
    this function takes as input some vector v = (v_(d-1), ..., v_(i-d)) and v_i = v_i , computes the phi_i, and based on the number
    of partitions returns a 'key' from 0, ..., number_angular_partitions-1 according to phi_i. 

    Since we are in the first orthant (assumption) the angular coordinate phi_i can only take values from 0 - 90. Hence, divide
    this range by the 'number_angular_partitions'. For example, key = 0 when phi_i lies in the range 0 - 90/number_angular_partitions (logic
    for the rest of the keys follows)

    The word 'key' might be misleading considering the whole application. In this function the 'key' has nothing to do with
    the partitioning MR_ANGLE key, it just provides the functionality to create it.

    Examples (after the computation of phi_i):
        number_angular_partitions=2 phi_i=50 : returns key=1. 50 belongs to 45-90
        number_angular_partitions=4 phi_i=10 : returns key=0. 10 belongs to 0-22.5
        number_angular_partitions=9 phi_i=79 : returns key=7. 79 belongs to 70-80

    Keyword arguments:
    :param number_angular_partitions : Number of 'partitions' of the angular coordinate phi_i. (solely the angular coordinate phi_i)
    :param v_i : Integer
    :param v : (tuple) Vector v = (v_(d-1), ..., v_(i-d))

    :returns a 'key' from 0, ..., number_of_keys-1 according to phi_i = arctan( || (v_d , ... , v_(i-1)) ||_2 / v_i ).
    """
    if v_i == 0:
        # angle equal to 90 degrees
        return number_angular_partitions-1

    # As explained before but in terms of radians
    angular_partition_range = (pi/2) / number_angular_partitions  

    return int(
        atan(
            sqrt(sum((v_j**2 for v_j in v))) / v_i
        ) // angular_partition_range
    )


def handle_input(*argv):
    """ Handles input.
    Keyword arguments:
    :param argv : arv[0] =  "SKYLINE OF x1 MIN, x2 MAX, ..., xd MIN" , argv[1] = "MR_ANGLE", argv[2] = "8"

    :returns the better functions (corresponding to this query as explained in depth in other comments), the dimension D,
        the algorithm name, and the partitioning paramater of the algorithm

    """
    query = argv[0]

    better_functions = []
    for s in query.replace(',', '').split():
        if s.lower() == 'min':
            better_functions.append(min)
        if s.lower() == 'max':
            better_functions.append(max)

    algo = argv[1]

    algo_param = int(argv[2])

    if len(better_functions) == 0:
        return None, None, None, None
    if algo == 'MR_ANGLE' and max in better_functions:
        # MAX queries not supported in this implementation of MR_ANGLE (TODO:)
        return None, None, None, None
    if not (algo == 'MR_DIM' or algo == 'MR_GRID' or algo == 'MR_ANGLE'):
        return None, None, None, None

    return better_functions, len(better_functions), algo, algo_param


def create_all_schemas(d):
    """ Creates all the necessary schemas that our application uses. Note: I will explain what this function returns
    giving examples. It's easier to understand with this approach.

    Keyword arguments:
    :param d: The dimension

    :returns 
        columns : A list of string dimension coordinates x1, x2. Ex. columns = ["x1", "x2", ...]
        csv_schema : The schema used by 'from_csv' function. Ex. csv_schema = "x1 INT, x2 INT, ..."
        skyline_schema : The skyline schema in PySpark types. The skyline in our application is nothing more
            than a list of points. But in PySpark terms it is an 'ArrayType'  of 'StructType' of
            a list of 'IntegerType' 'StructField's. Example when d = 2:
            point_schema = StructType(
                                [StructField("x1", IntegerType()), StructField("x2", IntegerType())]
                           )
            skyline_schema = ArrayType(point_schema)
            Obvious how to generalize this...
    """
    columns = [f"x{i}" for i in range(1,d+1)] # will contain ["x1", "x2", ...]

    point_field = []
    for coord in columns:
        point_field.append(StructField(coord, IntegerType()))
    skyline_schema = ArrayType(StructType(point_field)) 

    col_type = [] # will contain ["x1 INT", "x2 INT", ...]
    for c, t in zip(columns, ["INT" for _ in range(d)]):
        col_type.append(f"{c} {t}")
    csv_schema = ", ".join(col_type)

    return columns, csv_schema, skyline_schema



