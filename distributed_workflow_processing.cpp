#include <iostream>
#include <fstream>
#include <queue>
#include <iomanip>
#include "nlohmann/json.hpp"

using json = nlohmann::json;
using namespace std;


template<class T>
using Pair = pair<T,T>;

template<class T>
using TaskDependencies = map <Pair<T>,vector<T>>;

template<class T>
using VectorOfVectors = vector <vector<T>>;

template<class T>
using QueueOfPairs = queue <Pair<T>>;

template<class T>
using TaskDependencyDAG = pair <TaskDependencies<T>, VectorOfVectors<T>>;

// reads input from files and initialises workflow json object and number of workers
class WorkflowModel
{

public:
  json workflows;
  int num_workers;

  string workflows_input_file_name;
  string workers_input_file_name;
  string workflows_output_file_name;

  WorkflowModel(string _workflows_input_file_name, string _workers_input_file_name, string _workflows_output_file_name) {
    workflows_input_file_name = _workflows_input_file_name;
    workers_input_file_name = _workers_input_file_name;
    workflows_output_file_name = _workflows_output_file_name;
    readFromFile();
  }

  void readFromFile() {
    ifstream workflows_input_stream(workflows_input_file_name);
    ifstream worker_count_input_stream(workers_input_file_name);
    workflows_input_stream >> workflows;
    worker_count_input_stream >> num_workers;

    }

};

// exectues the distributed workflow
class WorkflowExecutor
{
private:
  int num_workers;
  json output_workflows;
  int workflows_size;

public:

  // creates a directed acyclic graph (DAG) from the workflow dependencies
  TaskDependencyDAG<int> getTaskDependencyDAG(json workflows) {

    TaskDependencies<int> task_dependencies;
    VectorOfVectors<int> incoming_edges(workflows_size);
    int workflow_cnt = 0;
    for(auto workflow: workflows){
        string workflow_name = workflow["name"].get<string>();
        incoming_edges[workflow_cnt].resize(workflow["tasks"].size());
        int task_cnt=0;
        map <string,int> task_num;
        for(auto task: workflow["tasks"]){
            incoming_edges[workflow_cnt][task_cnt]=0;
            task_num[task["name"]] = task_cnt++;
        }

        for(auto task: workflow["tasks"]){
            incoming_edges[workflow_cnt][task_num[task["name"]]] += task["dependencies"].size();
            for(auto dependencies: task["dependencies"]){
                task_dependencies[make_pair(workflow_cnt,task_num[dependencies])].push_back(task_num[task["name"]]);
            }

        }
        workflow_cnt++;
      }
    return make_pair(task_dependencies, incoming_edges);


  }

  // creates a topological sort for each workflow from the DAG
  VectorOfVectors<int> getTopologicalSort(TaskDependencyDAG<int> task_dependency_dag) {

    VectorOfVectors<int> topological_sort_workflows(workflows_size);
    QueueOfPairs<int> bfs_queue;

    TaskDependencies<int> task_dependencies;
    VectorOfVectors<int> incoming_edges(workflows_size);
    tie(task_dependencies, incoming_edges) = task_dependency_dag;

    for(int i=0; i<workflows_size; i++){
        for(int j=0; j< incoming_edges[i].size();j++){
            if(incoming_edges[i][j]==0)
                bfs_queue.push({i,j});
        }
    }

    while(bfs_queue.size()){
        pair<int,int> p = bfs_queue.front();
        bfs_queue.pop();
        int workflow = p.first;
        int task = p.second;
        topological_sort_workflows[workflow].push_back(task);
        for(auto dependent_task : task_dependencies[{workflow,task}]){
            incoming_edges[workflow][dependent_task]--;
            if(incoming_edges[workflow][dependent_task] == 0){
                bfs_queue.push({workflow,dependent_task});
            }
        }
    }

    return topological_sort_workflows;

  }

  // creates a scheduled_tasks queue which is interleafing of multiple workflow tasks
  QueueOfPairs<int> getScheduledTasks(json workflows, VectorOfVectors<int> topological_sort_workflows, int first_worflow, int last_workflow) {
    QueueOfPairs<int> scheduled_tasks;

    int f=1,j=0;
    while(f){
        f=0;
        for(int i = first_worflow; i < last_workflow; i++){
            if(workflows[i]["tasks"].size()>j){
                f=1;
                scheduled_tasks.push({topological_sort_workflows[i][j],i});
            }
        }
        j++;
    }
    return scheduled_tasks;
  }

  //processes the tasks parallelly and genrates output workflows
  void processAndPrepareOutput(json workflows, QueueOfPairs<int> scheduled_tasks, int first_worflow, int last_workflow) {
    long long int start_time = 1e18;
    for(auto workflow: workflows){
        long long int scheduled_at_time = workflow["scheduled_at"];
        start_time = min(start_time, scheduled_at_time);
    }
    priority_queue <pair<int,int>> workers;
    for(int i=1; i<=num_workers; i++){
        workers.push({start_time,i});
    }

    while(scheduled_tasks.size()){
        Pair <long long int> p = scheduled_tasks.front();
        scheduled_tasks.pop();
        int task = p.first;
        int workflow = p.second;

        p = workers.top();
        long long int epochtime_worker = p.first;
        int worker = p.second;
        workers.pop();
        int task_cost = workflows[workflow]["tasks"][task]["cost"];
        long long int workflow_scheduled_time = workflows[workflow]["scheduled_at"];
        long long int task_start_time = max(epochtime_worker, workflow_scheduled_time);
        output_workflows[workflow]["tasks"][task]["worker"]= [](int _worker) { return "worker" + to_string(_worker) ;}(worker);
        output_workflows[workflow]["tasks"][task]["started_at"]=task_start_time;
        output_workflows[workflow]["tasks"][task]["completed_at"]=task_start_time + task_cost;
        output_workflows[workflow]["tasks"][task].erase("description");
        output_workflows[workflow]["tasks"][task].erase("cost");
        output_workflows[workflow]["tasks"][task].erase("dependencies");
        workflows[workflow]["scheduled_at"] = task_start_time + task_cost;
        workers.push({task_start_time+task_cost,worker});

    }
    for(int i = first_worflow; i < last_workflow; i++){
        long long int completed_time = 0;
        for(int j = 0 ; j < output_workflows[i]["tasks"].size(); j++){
            long long int task_completion_time = output_workflows[i]["tasks"][j]["completed_at"];
            completed_time = max(completed_time, task_completion_time);
        }
        output_workflows[i]["completed_time"] = completed_time;
    }
    
  }

  //writes the output to output.json file
  void writeToFile() {
    ofstream workflows_output_stream("output.json");
    workflows_output_stream << std::setw(4) << output_workflows << std::endl;
  }

  // primary function which takes workflows and number of workers and input and generates the ouput
  void process_work(json _workflows, int _num_workers) {
    num_workers = _num_workers;
    workflows_size = _workflows.size();
    output_workflows = _workflows;
    auto topological_sort = getTopologicalSort(getTaskDependencyDAG(_workflows));
    auto first_half_scheduled_tasks = getScheduledTasks(_workflows, topological_sort, 0, (workflows_size+1)/2);
    processAndPrepareOutput(_workflows, first_half_scheduled_tasks, 0, (workflows_size+1)/2);
    auto second_half_scheduled_tasks = getScheduledTasks(_workflows, topological_sort, (workflows_size+1)/2, workflows_size);
    processAndPrepareOutput(_workflows, second_half_scheduled_tasks, (workflows_size+1)/2, workflows_size);
    writeToFile();
  }


};

int main()
{
    // create a JSON object
    WorkflowModel wf_model("workflows_input.json", "workers_input.txt", "output.json");
    WorkflowExecutor wf_executor;
    wf_executor.process_work(wf_model.workflows, wf_model.num_workers);
}
