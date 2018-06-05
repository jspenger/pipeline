################################################################################
# MIT License
#
# Copyright (c) 2018 Jonas Spenger
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
################################################################################
# description: Simple and lightweight tool for defining and running task and
# data pipelines, suitable for use in research and development.
# author: Jonas Spenger
# date: 01.2018
# TODO: Add caching
# TODO: Add parallelization
################################################################################
import itertools


class Pipeline(object):
    """Simple and lightweight tool for defining and running task and data
    pipelines, suitable for use in research and development.

    info: The structure of the pipeline is specified using lists, dicts and
    tuples. A dict specifies a stage (if it contains a 'function' key), a list
    is a list of stages, a tuple represents an option between the tuple values.
    The pipeline will run for all possible option combinations. A stage has 3
    required keys, 'in' 'out' and 'function'. 'in' represents which input sink
    to use, 'out' which output sink, and 'function' the function to use on the
    input data. 'kwargs' can also be added to the dict and will be passed to the
    function. The results are represented as a list of dictionaries, one
    dictionary per possible combinations. The entire data produced (unless
    overwritten) can be accessed in the results. Data can be input in the
    pipeline by calling the transform function using args or kwargs, if unnammed
    the data will correspond to the numbers 0, 1, ..., if named the data can
    be accessed through their names as found in 'in' and 'out'. If in and out
    sinks are ambigous, the order of the specification defines the order of
    processing (overwriting of data).

    tips: For namespace like behavior of inputs and outputs, a sub-pipeline's
    transform method can be used as the function of a stage.
    The 'filter_results' method can be used to conveniently choose the columns
    (i.e. keys) which to keep from the output, but also to flatten the result
    dictionary to make entries more accessible.
    The test module can be called through 'python pipeline.py' or
    'python pipeline.py -v'.

    example:
    >>> stage_1 = {'function': sum, 'in': [0], 'out': [1]}
    >>> stage_2 = {'function': lambda x, y: x * y, 'in': [1], 'out': [2], 'kwargs': {'y': (1.0, 2.0)}}
    >>> stage_3 = {'function' : lambda x: x * 2.0, 'in' : [1], 'out' : ['named_output']}
    >>> stage_4 = {'function' : lambda x: x, 'in' : ['named_output'], 'out' : [3]}
    >>> stage_5 = {'function' : lambda : [0,1,2,3], 'in' : [], 'out' : ['lambda_input']}
    >>> specification = ([stage_1, stage_2], [stage_1, stage_3, stage_4], [stage_5])
    >>> data = [0.0, 1.0, 2.0, 3.0]
    >>> pipeline = Pipeline(specification)
    >>> results = pipeline.transform(data)
    >>> results = pipeline.filter_results(results, columns=[0, 1, 2, 3, 'y', 'lambda_input'], flatten=True)
    >>> results[0]
    {0: [0.0, 1.0, 2.0, 3.0], 1: 6.0, 2: 6.0, 'y': 1.0}
    >>> results[1]
    {0: [0.0, 1.0, 2.0, 3.0], 1: 6.0, 2: 12.0, 'y': 2.0}
    >>> results[2]
    {0: [0.0, 1.0, 2.0, 3.0], 1: 6.0, 3: 12.0}
    >>> results[3]
    {0: [0.0, 1.0, 2.0, 3.0], 'lambda_input': [0, 1, 2, 3]}
    """

    def __init__(self, specification=None):
        self.__specification = specification

    def transform(self, *args, **kwargs):
        """Apply the pipeline(s) specified through specification on the input
        given through args and kwargs.
        """
        results = []
        for specification in self.__assemble(self.__specification):
            pipeline = self.__construct(specification)
            try:
                result = self.__run_pipeline(pipeline, *args, **kwargs)
                results.append(result)
            except Exception as e:
                print e
        return results

    def __run_pipeline(self, pipeline, *args, **kwargs):
        """Run a specific pipeline (no options) on args and kwargs.
        """
        data = kwargs
        data['pipeline'] = pipeline
        for i, arg in enumerate(args):
            data[i] = arg
        for stage in pipeline:
            inputs = []
            for inp in stage['in']:
                inputs.append(data[inp])
            if 'kwargs' in stage:
                outputs = stage['function'](*inputs, **stage['kwargs'])
            else:
                outputs = stage['function'](*inputs)
            if type(outputs) != tuple:
                outputs = (outputs, )
            for i, out in enumerate(stage['out']):
                data[out] = outputs[i]
        return data

    def __assemble(self, specification):
        """Given a specification, generate all possible combinations of
        specifications (with regards to options defined by tuples).
        """
        if type(specification) == tuple:  # if specification is an option:
            ret = []
            for i, val in enumerate(specification):
                ret.extend(self.__assemble(specification[i]))
            return ret
        elif type(specification) == dict:  # if specification is a stage:
            ret = {}
            for key in specification:
                ret[key] = self.__assemble(specification[key])
            return [dict(zip(ret.keys(), item)) for item in itertools.product(*ret.values())]
        elif type(specification) == list:  # if specification is a list:
            ret = []
            for i, val in enumerate(specification):
                ret.append(self.__assemble(specification[i]))
            return [list(item) for item in itertools.product(*ret)]
        else:  # if specification is atomic:
            return [specification]

    def __construct(self, specification):
        """Given a specification (with no options), generate a flat list of the
        stages in order of execution.
        """
        return self.__flatten_nested_list(specification)

    def __flatten_nested_list(self, nested_list):
        """Given a nested list, return a flat list.
        """
        ret = []
        for item in nested_list:
            if type(item) == list:
                ret.extend(self.__flatten_nested_list(item))
            else:
                ret.append(item)
        return ret

    @staticmethod
    def filter_results(results, columns=None, flatten=False):
        """Filter the results to only include the columns as specified through
        columns. The results can also be made flat, which makes every 'key'
        accessible on the first level in the dict.
        """
        def _flatten_nested_dict(nested_dict):
            """Given a nested dictionary, return a flat dictionary.
            """
            ret = []
            if type(nested_dict) == list:
                for i in nested_dict:
                    ret.extend([(key, value) for key, value in _flatten_nested_dict(i).items()])
            elif type(nested_dict) == dict:
                for k in nested_dict:
                    ret.append((k, nested_dict[k]))
                    if type(nested_dict[k]) in [dict, list]:
                        ret.extend([(key, value) for key, value in _flatten_nested_dict(nested_dict[k]).items()])
            return dict(ret)
        ret = []
        for result in results:
            if flatten == True:
                result = _flatten_nested_dict(nested_dict=result)  # flatten
            if columns != None:
                result = {k: result[k] for k in result if k in columns}  # filter by columns
            ret.append(result)
        return ret


if __name__ == "__main__":
    """Test the module.
    """
    import doctest
    doctest.testmod()
