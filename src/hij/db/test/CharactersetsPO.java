package hij.db.test;

public class CharactersetsPO {
    /**
     * CHARACTER_SET_NAME
     */
    private String cHARACTER_SET_NAME;
		
    /**
     * DEFAULT_COLLATE_NAME
     */
    private String dEFAULT_COLLATE_NAME;
		
    /**
     * DESCRIPTION
     */
    private String dESCRIPTION;
		
    /**
     * MAXLEN
     */
    private long mAXLEN;
		

    /**
     * CHARACTER_SET_NAME
    */
    public String getCHARACTER_SET_NAME() {
        return this.cHARACTER_SET_NAME;
    }		
		
    /**
     * CHARACTER_SET_NAME
     */
    public void setCHARACTER_SET_NAME(String cHARACTER_SET_NAME) {
        this.cHARACTER_SET_NAME = cHARACTER_SET_NAME;
    }
	 
    /**
     * DEFAULT_COLLATE_NAME
    */
    public String getDEFAULT_COLLATE_NAME() {
        return this.dEFAULT_COLLATE_NAME;
    }		
		
    /**
     * DEFAULT_COLLATE_NAME
     */
    public void setDEFAULT_COLLATE_NAME(String dEFAULT_COLLATE_NAME) {
        this.dEFAULT_COLLATE_NAME = dEFAULT_COLLATE_NAME;
    }
	 
    /**
     * DESCRIPTION
    */
    public String getDESCRIPTION() {
        return this.dESCRIPTION;
    }		
		
    /**
     * DESCRIPTION
     */
    public void setDESCRIPTION(String dESCRIPTION) {
        this.dESCRIPTION = dESCRIPTION;
    }
	 
    /**
     * MAXLEN
    */
    public long getMAXLEN() {
        return this.mAXLEN;
    }		
		
    /**
     * MAXLEN
     */
    public void setMAXLEN(long mAXLEN) {
        this.mAXLEN = mAXLEN;
    }
}
