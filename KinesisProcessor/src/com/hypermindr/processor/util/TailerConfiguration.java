package com.hypermindr.processor.util;

/**
 * Class to hold global configuration data, used in all tenants.
 * 
 * @author ricardo
 * 
 */
public class TailerConfiguration {

	private String awssecretkey;

	private String awsaccesskey;

	private String awskinesisEndpoint;

	private String awskinesisCname;

	private String awskinesisLocation;

	public String getAwssecretkey() {
		return awssecretkey;
	}

	public void setAwssecretkey(String awssecretkey) {
		this.awssecretkey = awssecretkey;
	}

	public String getAwsaccesskey() {
		return awsaccesskey;
	}

	public void setAwsaccesskey(String awsaccesskey) {
		this.awsaccesskey = awsaccesskey;
	}

	public String getAwskinesisEndpoint() {
		return awskinesisEndpoint;
	}

	public void setAwskinesisEndpoint(String awskinesisEndpoint) {
		this.awskinesisEndpoint = awskinesisEndpoint;
	}

	public String getAwskinesisCname() {
		return awskinesisCname;
	}

	public void setAwskinesisCname(String awskinesisCname) {
		this.awskinesisCname = awskinesisCname;
	}

	public String getAwskinesisLocation() {
		return awskinesisLocation;
	}

	public void setAwskinesisLocation(String awskinesisLocation) {
		this.awskinesisLocation = awskinesisLocation;
	}

}
