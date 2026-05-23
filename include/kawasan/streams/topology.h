#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <optional>

namespace kawasan {
namespace streams {

// Forward declarations
class ProcessorSupplier;
class StoreBuilder;

/**
 * Node type in the topology
 */
enum class NodeType {
    SOURCE,     // Reads from Kafka topic
    PROCESSOR,  // Transforms data
    SINK        // Writes to Kafka topic
};

/**
 * A node in the topology DAG
 */
struct TopologyNode {
    std::string name;
    NodeType type;
    std::vector<std::string> parents;
    std::vector<std::string> children;
    std::shared_ptr<ProcessorSupplier> processorSupplier;
    std::vector<std::string> stateStoreNames;
    
    // Source-specific
    std::vector<std::string> topics;
    
    // Sink-specific
    std::string outputTopic;
};

/**
 * Topology represents a directed acyclic graph (DAG) of stream processing nodes.
 * 
 * A topology consists of:
 * - Source nodes: Read from Kafka topics
 * - Processor nodes: Transform data (filter, map, aggregate, etc.)
 * - Sink nodes: Write to Kafka topics
 * - State stores: Persistent storage for stateful operations
 */
class Topology {
public:
    Topology() = default;
    ~Topology() = default;
    
    // Disable copy, allow move
    Topology(const Topology&) = delete;
    Topology& operator=(const Topology&) = delete;
    Topology(Topology&&) = default;
    Topology& operator=(Topology&&) = default;
    
    /**
     * Add a source node that reads from one or more topics
     * 
     * @param name Unique name for this source node
     * @param topics List of topics to read from
     */
    void addSource(std::string name, std::vector<std::string> topics);
    
    /**
     * Add a processor node that transforms data
     * 
     * @param name Unique name for this processor node
     * @param supplier Factory for creating processor instances
     * @param parents List of parent node names
     */
    void addProcessor(std::string name,
                     std::shared_ptr<ProcessorSupplier> supplier,
                     std::vector<std::string> parents);
    
    /**
     * Add a sink node that writes to a topic
     * 
     * @param name Unique name for this sink node
     * @param topic Output topic
     * @param parents List of parent node names
     */
    void addSink(std::string name,
                std::string topic,
                std::vector<std::string> parents);
    
    /**
     * Add a state store to the topology
     * 
     * @param storeName Unique name for the state store
     * @param builder Factory for creating the store
     * @param processorNames List of processors that will access this store
     */
    void addStateStore(std::string storeName,
                      std::shared_ptr<StoreBuilder> builder,
                      std::vector<std::string> processorNames);
    
    /**
     * Get all nodes in the topology
     */
    const std::map<std::string, TopologyNode>& nodes() const { return nodes_; }
    
    /**
     * Get state store builders
     */
    const std::map<std::string, std::shared_ptr<StoreBuilder>>& storeBuilders() const {
        return storeBuilders_;
    }
    
    /**
     * Validate the topology
     * 
     * Checks:
     * - No cycles
     * - No orphaned nodes
     * - All parent references are valid
     * - All state store references are valid
     * 
     * @return true if valid, false otherwise
     */
    bool validate() const;
    
    /**
     * Get a textual description of the topology
     */
    std::string describe() const;
    
private:
    std::map<std::string, TopologyNode> nodes_;
    std::map<std::string, std::shared_ptr<StoreBuilder>> storeBuilders_;
    
    // Helper methods
    bool hasCycle() const;
    bool hasOrphanedNodes() const;
    void dfsVisit(const std::string& nodeName,
                 std::map<std::string, int>& visited,
                 bool& hasCycle) const;
};

/**
 * Abstract base class for creating processor instances
 */
class ProcessorSupplier {
public:
    virtual ~ProcessorSupplier() = default;
    
    /**
     * Create a new processor instance
     * 
     * This method is called once per task to create a processor
     * that will handle records for a specific partition subset.
     */
    virtual std::shared_ptr<void> get() = 0;
};

/**
 * Abstract base class for creating state store instances
 */
class StoreBuilder {
public:
    virtual ~StoreBuilder() = default;
    
    /**
     * Get the name of the store
     */
    virtual std::string name() const = 0;
    
    /**
     * Create a new state store instance
     * 
     * @param stateDir Directory to store persistent state
     * @return Pointer to the created store
     */
    virtual std::shared_ptr<void> build(const std::string& stateDir) = 0;
    
    /**
     * Whether this store is persistent (backed by disk)
     */
    virtual bool isPersistent() const = 0;
};

} // namespace streams
} // namespace kawasan
