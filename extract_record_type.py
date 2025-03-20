import marvin
import json
from rich import print
from rich.progress import track
from pydantic import BaseModel, Field

all_flows = {}
with open('success_source_target_with_fid_full.jsonl', 'r') as f:
    for line in track(f):
        obj = json.loads(line)
        flow_id = obj.get('flow_id', 'UNK')
        all_flows[flow_id] = obj

    print("Loaded {} flows".format(len(all_flows)))

record_types = ['customers', 'orders', 'products', 'inventory', 'fulfillments',
                'returns', 'refunds', 'payments', 'transactions', 'accounts',
                'contacts', 'leads', 'opportunities', 'cases', 'solutions',
                'campaigns', 'activities', 'events', 'tasks', 'notes', 'attachments',
                'contracts', 'assets', 'services', 'service_requests', 'service_tasks',
                'service_appointments', 'service_contracts', 'service_products',
                'employees', 'payroll', 'benefits', 'time_off', 'compensation',
                'performance', 'recruiting', 'onboarding', 'offboarding', 'learning',
                'tickets', 'conversations', 'invoices', 'payments', 'expenses',
                'inventory levels', 'purchase orders', 'sales orders', 'quotes',
                'price updates', 'shipping updates', 'vendors', 'sales orders',
                'purchase orders', 'credit memos', 'inventory adjustments',
                'general ledger entries', 'leads', 'opportunities',
                'accounts','campaigns', 'quotes', 'job postings',
                'applications', 'support tickets', 'agents', 'comments',
                'ticket priorities', 'ticket statuses', 'ticket types','chart of accounts',
                'general ledger transactions', 'journal entries',
                'bank transactions', 'tax calculations', 'vendor bills',
                'payment reconcilliations', 'shipments', 'tracking numbers',
                'carrier rates', 'shipping labels', 'shipping manifests',
                'delivery status updates', 'subscriptions', 'refunds', 'payment failures']

class RecordTypes(BaseModel):
    source_record: str = Field(..., title='''The export record type from which data is fetched. Can be one of the
                               following values: {}'''.format(' '.join(set(record_types)))),
    destination_record:str = Field(..., title='''The import record type that is created or updated. Can be one
                                   of the following values: {}'''.format(' '.join(set(record_types))))

def main():
    filename = 'sorted_flows_full_raw.jsonl'
    fp = open(filename, 'r')
    cx = 0
    ft = open('source_target__record_type_with_fid_full.jsonl', 'a')
    #for line in track(fp):
    for line in fp:
        cx += 1
        if cx < 6121: 
            print("Skipping ", cx)
            continue
        flow_obj = json.loads(line)
        #print("Flow object={}".format(flow_obj))
        flow_id = flow_obj.get('flow_id')
        if not flow_id: continue

        obj = all_flows.get(flow_id, None)
        if not obj: continue
        #print("Object retrieved is {}".format(obj))

        flow_description = flow_obj.get('flow_description', '')
        flow_name = flow_obj.get('name', '')
        resources = flow_obj.get('resources', [])
        content = []
        #content.append("Flow name: "+flow_name)

        #content.append("Flow description:" + flow_description)
        for resource in resources:
            is_lookup = resource.get('is_lookup', False)
            if is_lookup:
                resource_type = 'lookup'
            else:
                resource_type = resource.get('type', '')
            content.append(resource_type)
            name = resource.get('name', '')
            content.append("Name: "+name)
            description = resource.get('description', '')
            content.append("Description:"+description)

        final_content = '\n'.join(content)
        print("Final content=", final_content)

        result = determine_record_types(final_content)
        if result.source_record is None:
            source_record = 'UNK'
        else:
            source_record = str(result.source_record)
        if result.destination_record is None:
            destination_record = 'UNK'
        else:
            destination_record = str(result.destination_record)
        print(flow_obj)
        print(result)
        if result:

            ft.write(json.dumps({'flow_id': flow_id,
                                 'flow_name': flow_name,
                                 'flow_description': flow_description,
                                 'source': obj['source'],
                                 'destination': obj['destination'],
                                 'source_record': source_record,
                                 'destination_record': destination_record,
                                })+'\n')
        print("Processed {} flows".format(cx))
        #if cx==100: break
    ft.close()

def determine_record_types(content):
    results = marvin.extract(content, RecordTypes)
    if len(results) > 0:
        return results[0]

if __name__ == "__main__":
    main()
