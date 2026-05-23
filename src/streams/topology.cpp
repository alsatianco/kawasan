#include "kawasan/streams/topology.h"
#include <sstream>
#include <set>
#include <algorithm>

namespace kawasan {
namespace streams {

void Topology::addSource(std::string name, std::vector<std::string> topics) {
    if (nodes_.find(name) != nodes_.end()) {
        throw std::runtime_error("Node with name '" + name + "' already exists");
    }
    
    TopologyNode node;
    node.name = name;
    node.type = NodeType::SOURCE;
    node.topics = std::move(topics);
    
    nodes_[name] = std::move(node);
}

void Topology::addProcessor(std::string name,
                           std::shared_ptr<ProcessorSupplier> supplier,
                           std::vector<std::string> parents) {
    if (nodes_.find(name) != nodes_.end()) {
        throw std::runtime_error("Node with name '" + name + "' already exists");
    }
    
    // Validate parent nodes exist
    for (const auto& parent : parents) {
        if (nodes_.find(parent) == nodes_.end()) {
            throw std::runtime_error("Parent node '" + parent + "' does not exist");
        }
    }
    
    TopologyNode node;
    node.name = name;
    node.type = NodeType::PROCESSOR;
    node.parents = parents;
    node.processorSupplier = std::move(supplier);
    
    // Add this node as a child to all parents
    for (const auto& parent : parents) {
        nodes_[parent].children.push_back(name);
    }
    
    nodes_[name] = std::move(node);
}

void Topology::addSink(std::string name,
                      std::string topic,
                      std::vector<std::string> parents) {
    if (nodes_.find(name) != nodes_.end()) {
        throw std::runtime_error("Node with name '" + name + "' already exists");
    }
    
    // Validate parent nodes exist
    for (const auto& parent : parents) {
        if (nodes_.find(parent) == nodes_.end()) {
            throw std::runtime_error("Parent node '" + parent + "' does not exist");
        }
    }
    
    TopologyNode node;
    node.name = name;
    node.type = NodeType::SINK;
    node.parents = parents;
    node.outputTopic = std::move(topic);
    
    // Add this node as a child to all parents
    for (const auto& parent : parents) {
        nodes_[parent].children.push_back(name);
    }
    
    nodes_[name] = std::move(node);
}

void Topology::addStateStore(std::string storeName,
                             std::shared_ptr<StoreBuilder> builder,
                             std::vector<std::string> processorNames) {
    if (storeBuilders_.find(storeName) != storeBuilders_.end()) {
        throw std::runtime_error("State store '" + storeName + "' already exists");
    }
    
    // Validate processor nodes exist
    for (const auto& procName : processorNames) {
        auto it = nodes_.find(procName);
        if (it == nodes_.end()) {
            throw std::runtime_error("Processor node '" + procName + "' does not exist");
        }
        if (it->second.type != NodeType::PROCESSOR) {
            throw std::runtime_error("Node '" + procName + "' is not a processor");
        }
        
        // Add store to processor's state store list
        it->second.stateStoreNames.push_back(storeName);
    }
    
    storeBuilders_[storeName] = std::move(builder);
}

bool Topology::validate() const {
    // Check for cycles
    if (hasCycle()) {
        return false;
    }
    
    // Check for orphaned nodes (nodes with no path to a sink)
    if (hasOrphanedNodes()) {
        return false;
    }
    
    // Check state store references
    for (const auto& [nodeName, node] : nodes_) {
        for (const auto& storeName : node.stateStoreNames) {
            if (storeBuilders_.find(storeName) == storeBuilders_.end()) {
                return false;
            }
        }
    }
    
    return true;
}

bool Topology::hasCycle() const {
    // Use DFS with colors: 0=white, 1=gray, 2=black
    std::map<std::string, int> visited;
    for (const auto& [name, _] : nodes_) {
        visited[name] = 0;
    }
    
    bool hasCycle = false;
    for (const auto& [name, _] : nodes_) {
        if (visited[name] == 0) {
            dfsVisit(name, visited, hasCycle);
            if (hasCycle) {
                return true;
            }
        }
    }
    
    return false;
}

void Topology::dfsVisit(const std::string& nodeName,
                       std::map<std::string, int>& visited,
                       bool& hasCycle) const {
    visited[nodeName] = 1;  // Gray (being processed)
    
    auto it = nodes_.find(nodeName);
    if (it != nodes_.end()) {
        for (const auto& child : it->second.children) {
            if (visited[child] == 1) {
                // Back edge found (gray node)
                hasCycle = true;
                return;
            }
            if (visited[child] == 0) {
                dfsVisit(child, visited, hasCycle);
                if (hasCycle) {
                    return;
                }
            }
        }
    }
    
    visited[nodeName] = 2;  // Black (done)
}

bool Topology::hasOrphanedNodes() const {
    // Find all sink nodes and terminal processors (processors with no children)
    std::set<std::string> reachableFromSource;
    std::set<std::string> reachesAnEndpoint;
    
    // Forward pass: find all nodes reachable from sources
    std::vector<std::string> queue;
    for (const auto& [name, node] : nodes_) {
        if (node.type == NodeType::SOURCE) {
            queue.push_back(name);
            reachableFromSource.insert(name);
        }
    }
    
    while (!queue.empty()) {
        std::string current = queue.back();
        queue.pop_back();
        
        auto it = nodes_.find(current);
        if (it != nodes_.end()) {
            for (const auto& child : it->second.children) {
                if (reachableFromSource.find(child) == reachableFromSource.end()) {
                    reachableFromSource.insert(child);
                    queue.push_back(child);
                }
            }
        }
    }
    
    // Backward pass: find all nodes that reach a sink or terminal processor
    queue.clear();
    for (const auto& [name, node] : nodes_) {
        // A node is an endpoint if it's a SINK or a PROCESSOR with no children
        if (node.type == NodeType::SINK || 
            (node.type == NodeType::PROCESSOR && node.children.empty())) {
            queue.push_back(name);
            reachesAnEndpoint.insert(name);
        }
    }
    
    while (!queue.empty()) {
        std::string current = queue.back();
        queue.pop_back();
        
        auto it = nodes_.find(current);
        if (it != nodes_.end()) {
            for (const auto& parent : it->second.parents) {
                if (reachesAnEndpoint.find(parent) == reachesAnEndpoint.end()) {
                    reachesAnEndpoint.insert(parent);
                    queue.push_back(parent);
                }
            }
        }
    }
    
    // A node is orphaned if it's reachable from a source but doesn't reach an endpoint
    for (const auto& name : reachableFromSource) {
        if (reachesAnEndpoint.find(name) == reachesAnEndpoint.end()) {
            return true;  // Found an orphaned node
        }
    }
    
    return false;
}

std::string Topology::describe() const {
    std::ostringstream oss;
    oss << "Topology:\n";
    oss << "  Nodes: " << nodes_.size() << "\n";
    oss << "  State Stores: " << storeBuilders_.size() << "\n\n";
    
    // Group nodes by type
    std::vector<std::string> sources, processors, sinks;
    for (const auto& [name, node] : nodes_) {
        switch (node.type) {
            case NodeType::SOURCE:
                sources.push_back(name);
                break;
            case NodeType::PROCESSOR:
                processors.push_back(name);
                break;
            case NodeType::SINK:
                sinks.push_back(name);
                break;
        }
    }
    
    // Print sources
    if (!sources.empty()) {
        oss << "  Sources:\n";
        for (const auto& name : sources) {
            const auto& node = nodes_.at(name);
            oss << "    " << name << " (topics: [";
            for (size_t i = 0; i < node.topics.size(); ++i) {
                if (i > 0) oss << ", ";
                oss << node.topics[i];
            }
            oss << "])\n";
            if (!node.children.empty()) {
                oss << "      --> [";
                for (size_t i = 0; i < node.children.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << node.children[i];
                }
                oss << "]\n";
            }
        }
        oss << "\n";
    }
    
    // Print processors
    if (!processors.empty()) {
        oss << "  Processors:\n";
        for (const auto& name : processors) {
            const auto& node = nodes_.at(name);
            oss << "    " << name;
            if (!node.stateStoreNames.empty()) {
                oss << " (stores: [";
                for (size_t i = 0; i < node.stateStoreNames.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << node.stateStoreNames[i];
                }
                oss << "])";
            }
            oss << "\n";
            if (!node.children.empty()) {
                oss << "      --> [";
                for (size_t i = 0; i < node.children.size(); ++i) {
                    if (i > 0) oss << ", ";
                    oss << node.children[i];
                }
                oss << "]\n";
            }
        }
        oss << "\n";
    }
    
    // Print sinks
    if (!sinks.empty()) {
        oss << "  Sinks:\n";
        for (const auto& name : sinks) {
            const auto& node = nodes_.at(name);
            oss << "    " << name << " (topic: " << node.outputTopic << ")\n";
        }
    }
    
    return oss.str();
}

} // namespace streams
} // namespace kawasan
