package com.dsc.mtrc.internal;


import java.util.Hashtable;

import javax.naming.AuthenticationException;
import javax.naming.AuthenticationNotSupportedException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

 
public class Ldap {
	 
	//domain name constant values
	public static final String DSCCORP_DOMAIN = "dsccorp";
	public static final String COLONIALHEIGHTS_DOMAIN  = "ColonialHeights";
	public static final String DSCLOGISTICS_DOMAIN = "dsclogistics";
	
	//ldap URSs
	public static final String DSCCORP_URL = "ldap://192.168.43.110/DC=dsccorp,DC=net";
	public static final String COLONIALHEIGHTS_URL  = "ldap://192.168.99.25/DC=ColonialHeights,DC=dsccorp,DC=net";
	public static final String DSCLOGISTICS_URL = "ldap://192.168.2.1/DC=dsclogistics,DC=dsccorp,DC=net";
	
	public JSONObject authenticateLDAPUser(String username, String password, String domain) throws JSONException
	{
		
		JSONObject retJson = new JSONObject();
		JSONObject errJson = new JSONObject();
		StringBuffer sb = new StringBuffer();		 	 	 
	    //String msg="";
	    String err="";
	  	String url = "";
	    String domain_name = "";
	    //if domain name isn't passed we assume it's dsclogistics
	    if(domain==null||domain.equals("")||domain.toUpperCase().equals(DSCLOGISTICS_DOMAIN.toUpperCase()))
	    {
	    	url = DSCLOGISTICS_URL;
		    domain_name = DSCLOGISTICS_DOMAIN;
	    }
	    else if((domain!=null||!domain.equals(""))&&domain.toUpperCase().equals(COLONIALHEIGHTS_DOMAIN.toUpperCase()))
	    {
	    	url= COLONIALHEIGHTS_URL;
			domain_name = COLONIALHEIGHTS_DOMAIN;	  
	    }	    	  
		else if((domain!=null||!domain.equals(""))&&domain.toUpperCase().equals(DSCCORP_DOMAIN.toUpperCase()))
		{
	  	  url=DSCCORP_URL;
	  	  domain_name = DSCCORP_DOMAIN;
		}
		else
		{
			errJson.put("result", "FAILED");
			errJson.put("resultCode", 300);
			errJson.put("message", "Invalid Domain Name");
			return errJson;
		}
		
	    	
	    
    
	    Hashtable<String, String> env = new Hashtable<String, String>();
	   
    
	    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, url);
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, new String(domain_name + "\\"+username));
		env.put(Context.SECURITY_CREDENTIALS, password);
		
		DirContext ctx = null;	 
        NamingEnumeration results = null; 
        try {
		    ctx = new InitialDirContext(env);
			System.out.println(ctx.getEnvironment());
			//rb=Response.ok(ctx.getEnvironment().toString()).build();	

	         	
		  SearchControls controls = new SearchControls();
          controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
			String[] attrIDs = { "distinguishedName",
				"sn",
				"givenname",
				"mail",
				"sAMAccountName",
				"objectclass",
				"telephonenumber"};
			  controls.setReturningAttributes(attrIDs);
          results = ctx.search("", "(sAMAccountName="+username+")", controls);


          if (results.hasMore()) {
				Attributes attrs = ((SearchResult) results.next()).getAttributes();
				System.out.println("distinguishedName "+ attrs.get("distinguishedName"));
				String [] dname=attrs.get("distinguishedName").toString().split(",");
				String [] cnname=dname[0].split("=");
				String fld=null;
				fld=attrs.get("givenname").toString();
				String[] parts = fld.split(":");
				String fname=parts[1].trim();
				fld=attrs.get("sn").toString();
			    parts = fld.split(":");		 
				String lname=parts[1].trim();		  				
				fld=attrs.get("mail").toString();
			    parts = fld.split(" ");	
				String email=parts[1].trim();
				JSONObject tempJson = new JSONObject();
				tempJson.put("name",cnname[1]);
				tempJson.put("first_name",fname);
				tempJson.put("last_name", lname);
				tempJson.put("email",email);
				retJson.put("result", "SUCCESS");
				retJson.put("resultCode", 0);
				retJson.put("message", "");
				retJson.put("DSCAuthenticationSrv", tempJson);
			}


			ctx.close();

		} catch (AuthenticationNotSupportedException ex) {
			err="The authentication is not supported by the server";

		} catch (AuthenticationException ex) {
			err=err+"incorrect password or username";
		} catch (NamingException ex) {
			err=err+"error when trying to create the context";
		}	
		      
		 
		catch (Exception ex)
		{
			err=err+"Error Getting Attrs";
	    }
		if (err.length()> 0)
		{
			//sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +err +"\"}");
			errJson.put("result", "FAILED");
			errJson.put("resultCode", 300);
			errJson.put("message", err);
			return errJson;
	    }
  
    //System.out.println(sb.toString());
    return retJson;		
	    
	    

	}	
}
	
