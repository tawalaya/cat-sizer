
import json
from util.utils import get_recursively
from sizer.regression_sizer import RegressionSizer
from sizer.workflow_sizer import WorkflowSizer
class State:
    def __init__(self, name, arn, state_dict, start, end):
        self.name = name
        self.arn = arn
        self.state_dict = state_dict

        self.start = start
        self.end = end
        self.next = []

    def to(self, other):
        self.next.append(other)



def parse_state_machine(definition, next=None):
    states = []
    items = reversed(list(definition['States'].items()))

    for item in items:
        state_name, state_dict = item[0], item[1]
        end = state_dict.get('End', False)
        state = State(name=state_name, arn=state_dict.get('Resource', ''), state_dict=state_dict, start=state_name == definition['StartAt'], end=end)
        name_to_state[state_name] = state

        if state_dict['Type'] == 'Parallel':
            branch_next = name_to_state[state_dict['Next']]
            for branch in state_dict['Branches']:
                states += parse_state_machine(branch, branch_next)

            for branch in state_dict['Branches']:
                state.to(name_to_state[branch['StartAt']])

        else:
            if next and end:
                state.to(next)
            elif 'Next' in state_dict:
                state.to(name_to_state[state_dict['Next']])
        states.append(state)

    return states







if __name__ == '__main__':
    import sys
    argv =  sys.argv[1:]
    #defaults
    file = None
    arn = None
    payloads = None
    elat_constraint=2000
    sizes = [128,256,512,1024,2048,3096]
    name_to_state = {}

    if len(argv) <= 1:
        print("Usage: <workflow-arn> <workflow.json> <elat_constraint> <payloads> <sizes>")
        exit(0)

    #TODO: needs content validation ;)
    arn = argv[0]
    file = argv[1]
    if len(argv) > 2:
        elat_constraint=argv[2]
    if len(argv) > 3:
        with open(argv[3]) as f:
            payloads = json.load(f)
    
    if len(argv) > 4:
        sizes = argv[4:]
    

    with open(file) as f:
        json_content = json.load(f)

    print(get_recursively(json_content, 'Resource'))
    states = list(reversed(parse_state_machine(json_content)))



    lambda_client = boto3.client('lambda')
    lambdas = []
    transitions = []
    states_list = []
    for state in states:
        states_list.append(
            {'name': state.name, 'arn':state.arn }
        )
        lambdas.append(state.arn)
        for next in state.next:
            transitions.append({
                'from': state.name,
                'to': next.name
            })

    initial_state = name_to_state[json_content['StartAt']]
    
    lambdas = set(lambdas)

    #TODO: force user interaction to halt if we do not want to sample

    total_cost = 0
    total_duration = 0
    #generate induvidual models
    for f in lambdas:
        p = {}
        if payloads is not None and f in payloads: 
            p = payloads[f] 
        sizer = RegressionSizer(lambda_arn=f,payload=p,balanced_weight=0.5,sample_runs=5,memory_sizes=sizes)
        result, logs, popt, cost = sizer.configure_function()
        res = {
            'arn':f,
            'memorySize': result.memory_size,
            'cost': result.cost,
            'duration': result.duration,
            'total_cost':cost,
        }
        print(json.dumps(res, indent=4))
        total_cost += total_cost
        total_duration += result.duration
    
    wfs = WorkflowSizer(arn,elat_constraint)
    
    sizes,elat,cost = wfs.run()
    res = {
        'arn':arn,
        'cost':cost,
        'elat':elat,
        'total_cost':total_cost,
        'total_duration':total_duration,
        'sizes':sizes,
    }
    print(json.dumps(res, indent=4))

