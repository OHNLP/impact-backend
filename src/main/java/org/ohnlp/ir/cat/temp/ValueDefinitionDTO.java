package org.ohnlp.ir.cat.temp;

public class ValueDefinitionDTO {
    private FHIRValueLocationPath valuePath;
    private ValueRelationType type;
    private String[] values;

    public FHIRValueLocationPath getValuePath() {
        return valuePath;
    }

    public void setValuePath(FHIRValueLocationPath valuePath) {
        this.valuePath = valuePath;
    }

    public ValueRelationType getType() {
        return type;
    }

    public void setType(ValueRelationType type) {
        this.type = type;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }
}
