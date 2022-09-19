package org.ohnlp.ir.cat.temp;



import java.util.List;
import java.util.UUID;

public class CriterionDefinitionDTO {
    private UUID node_id;
    private NodeType node_type;
    private EntityDefinitionDTO entity;
    private List<CriterionDefinitionDTO> children;

    public UUID getNode_id() {
        return node_id;
    }

    public void setNode_id(UUID node_id) {
        this.node_id = node_id;
    }

    public NodeType getNode_type() {
        return node_type;
    }

    public void setNode_type(NodeType node_type) {
        this.node_type = node_type;
    }

    public EntityDefinitionDTO getEntity() {
        return entity;
    }

    public void setEntity(EntityDefinitionDTO entity) {
        this.entity = entity;
    }

    public List<CriterionDefinitionDTO> getChildren() {
        return children;
    }

    public void setChildren(List<CriterionDefinitionDTO> children) {
        this.children = children;
    }
}
