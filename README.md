# pipeline
Simple and lightweight tool for defining and running task and data pipelines, suitable for use in research and development.

## Examples
```
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
```

## Author
* Jonas Spenger

## License
* This project is licensed under the MIT License - see the LICENSE file for details.
