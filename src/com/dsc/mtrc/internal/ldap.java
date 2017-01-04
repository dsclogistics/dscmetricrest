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
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

 
public class ldap {
	 
	
	public Response ldap(JSONObject inputJsonObj) throws JSONException {
		    Response rb = null;
		    StringBuffer sb = new StringBuffer();		 	 	 
		    String msg="";
		    String err="";
		    String username="";
		    String password="";
		    
		    if ((inputJsonObj.has("username"))  && (inputJsonObj.has("password")))
		    {
		    	msg="";
		    }
		    else
		    {
				err="Json elements username and password is required for this API ";
				sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +err +"\"}");		
			 	rb=Response.ok(sb.toString()).build();
			//	System.out.println(sb.toString());
				return rb;
		    }
		    			
		    		 
		     if ((inputJsonObj.get("username").toString().length()  > 0 ) &&
		         (inputJsonObj.get("password").toString().length()  > 0 ))
		     {
		    	  username= inputJsonObj.get("username").toString();
		    	  password=inputJsonObj.get("password").toString();

	 			//String url = "ldap://192.168.2.1/OU=Desktop,OU=User Accounts,OU=CORPORATE HEADQUARTERS\\, IL,DC=dsclogistics,DC=dsccorp,DC=net";
	 			String url="ldap://192.168.2.1/DC=dsclogistics,DC=dsccorp,DC=net";//192.168.43.110
	 			//String url="ldap:DC=dsclogistics,DC=dsccorp,DC=net";
	 			Hashtable env = new Hashtable();
	 			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
	 			env.put(Context.PROVIDER_URL, url);
	 			env.put(Context.SECURITY_AUTHENTICATION, "simple");
	 			env.put(Context.SECURITY_PRINCIPAL, new String("dsclogistics" + "\\"+username));
	 			env.put(Context.SECURITY_CREDENTIALS, password);
	 			
	 			DirContext ctx = null;	 
	 	        NamingEnumeration results = null; 

	 			try {
	 			    ctx = new InitialDirContext(env);
	 			//	System.out.println("connected");
	 				// System.out.println(ctx.getEnvironment());
	 				rb=Response.ok(ctx.getEnvironment().toString()).build();	
 
	 		         
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
		            //  results = ctx.search("", "(objectclass=person)", controls);
		              results = ctx.search("", "(sAMAccountName="+username+")", controls);
		

		              if (results.hasMore()) {
		  				Attributes attrs = ((SearchResult) results.next()).getAttributes();
		  			//	System.out.println("distinguishedName "+ attrs.get("distinguishedName"));
		  				String [] dname=attrs.get("distinguishedName").toString().split(",");
		  				String [] cnname=dname[0].split("=");
		  				String fld=null;
		  				fld=attrs.get("givenname").toString();
		  				String[] parts = fld.split(":");

		  				
		  		//		System.out.println("givenname "+ attrs.get("givenname"));
		  				String fname=parts[1].trim();
		  			//	System.out.println("FirstName "+ parts[1].trim());
		  				fld=attrs.get("sn").toString();
		  			    parts = fld.split(":");	
		  			//	System.out.println("sn "+ attrs.get("sn"));
		  			//	System.out.println("LastName "+ parts[1].trim());	 
		  				String lname=parts[1].trim();		  				
		  			//	System.out.println("mail "+ attrs.get("mail"));
		  				fld=attrs.get("mail").toString();
		  			    parts = fld.split(" ");	
		  			//	System.out.println("YourEmail "+ parts[1].trim());
		  				String email=parts[1].trim();
		  			//	System.out.println("telephonenumber "+ attrs.get("telephonenumber"));
		  				 msg= ",\"DSCAuthenticationSrv\":";  
		  				msg=msg+"{\"name\":\""+cnname[1]+"\",\"first_name\":\""+fname +
		  					"\",\"last_name\":\""+lname +
		  					"\",\"email\":\""+email+"\"}";
		  				sb.append("{\"result\":\"SUCCESS\",\"resultCode\":0,\"message\":\"\""+msg+"}");	
		  			}
 

	 				ctx.close();

	 			} catch (AuthenticationNotSupportedException ex) {
	 			//	System.out.println("The authentication is not supported by the server");
	 				err="The authentication is not supported by the server";
	 		   		// sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +msg +"\"}");
	 			} catch (AuthenticationException ex) {
	 				err=err+"incorrect password or username";
	 		   		// sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +msg +"\"}");
	 			//	System.out.println("incorrect password or username");
	 			} catch (NamingException ex) {
	 				err=err+"error when trying to create the context";
	 		   		//sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +msg +"\"}");	 				
	 			//	System.out.println("error when trying to create the context"+ex.getMessage().toString());
	 			}	
	 			      
	 			 
	 			catch (Exception ex)
	 			{
	 				// System.out.println("Error Getting Attrs:"+ex.getMessage().toString());
	 				err=err+"Error Getting Attrs";
	 		   			 				
	            }
	 			if (err.length()> 0){sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +err +"\"}");}
	        }
	     
		else
		
		{
			err="Json elements username and/or password is empty. Please try with valid user/password for this API ";
			sb.append("{\"result\":\"FAILED\",\"resultCode\":300,\"message\":\"" +err +"\"}");
		
		}
	 	   rb=Response.ok(sb.toString()).build();
	 //	  System.out.println(sb.toString());
	 	  return rb;
       	 
 
	}      
	
}
	
