# Containers Orchestrator

Container orchestration project with the aim to reproduce a minimal Kubernetes solution. The goal is to learn how container orchestration systems work while providing a working and operable solution.
Project limitation: workers can only run on Linux systems because the stats retrieval library ([goprocinfo](https://github.com/c9s/goprocinfo)) is limited to Linux.

## Architecture

### Manager
The manager is the main entry point to the system, it communicates with workers and stores the desired state of containers. It is equipped with a scheduler which selects the best fitting worker node to execute a **task** (container action).
It is responsible to ensure workers carry out their affected tasks, it does so by checking workers' tasks state regularly and planning tasks re-scheduling in case of discrepancies. The manager sends commands to its workers using their REST API.

### Worker / Node
Worker nodes are responsible of starting and stopping Docker containers, they do so by using the [Docker Go library](https://github.com/docker/docker). They expose their node's statistics (Linux procinfo) for scheduling purposes.

### Scheduling
The orchestrator system supports multiple worker nodes. In order to select a worker for a task, the manager comes with two scheduling algorithms, which can be selected at startup:
- Round-robin: alternate between each available worker
- EPVM: select the most suitable worker in terms of available resources for the given task requirements

### Storage
Both manager and worker have access to two storage provider:
- In memory
- Persisted: writes data files to disk

## Usage

A CLI client is provided to communicate with the orchestration manager. It is a REST API caller, meaning it is also possible to send commands to the manager using its API.

### Manager

Start manager with 2 registered workers:
`manager -p 8080 -st persisted -sct epvm -w worker1:80 -w worker2:80`

Send commands to the Manager:
`client --host managerhost -p 8080`

From the spawned CLI:
- Start a task from a file: `> start path/to/specs.json`
- Stop a task: `> stop c31da4c1-427b-4066-be93-d4577ad83544`
- Get task details: `> get c31da4c1-427b-4066-be93-d4577ad83544`
- List tasks from all workers: `> list`
- List worker nodes: `> list-nodes`

### Worker

Start a worker:
`worker -n worker1 -p 80 -st persisted`

## Planned evolution

This project is the foundation to building a hosting provider platform that enables developers to easily deploy web applications and expose them online. It would work with existing Dockerfiles but allow without them (auto generation based on project language). Just link the code repository and see the application online.