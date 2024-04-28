from math import radians, sin, cos, acos

def great_circle_distance(
    lat1: float, 
    long1: float, 
    lat2: float, 
    long2: float, 
    radius: float = 6371.009, 
    input_format: str = 'degrees'
) -> float:
    """
    Description: Computes the great circle distance of two points on a sphere 
    using the spherical law of cosines. 

    Additional info: https://en.wikipedia.org/wiki/Great-circle_distance

    Parameters:
    - long1 (float): The longitude of point 1.
    - lat1 (float): The latitude of point 1.
    - long2 (float): The longitude of point 2.
    - lat2 (float): The latitude of point 2.
    - radius (float): Radius of the sphere. 
      Default 6371.009 - the mean radius of Earth in km.
    - input_format (string): Either 'degrees' or 'radians'. Default 'degrees'

    Returns:
    - float: The great-circle distance between point 1 and point 2.
    """

    # check for valid input
    if input_format not in ['degrees', 'radians']:
        raise ValueError("Invalid format: Must be 'degrees' or 'radians'")

    # convert coordinates if specified
    if input_format == 'degrees':
        long1, lat1 = radians(long1), radians(lat1)
        long2, lat2 = radians(long2), radians(lat2)


    # compute central angle using spherical law of cosines
    delta_lambda = abs(long1 - long2)
    delta_sigma = acos(sin(lat1)*sin(lat2) + cos(lat1)
                       * cos(lat2)*cos(delta_lambda))

    # return arclength of the sphere
    return delta_sigma * radius