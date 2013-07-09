package br.com.dishup.environment;

public class LoadCodedataParametersEnvironment {
	
	private final String filePathPais = "geographic//pais.txt";
	private final String filePathEstado = "geographic//estado.txt";
	private final String filePathCidade = "geographic//cidade.txt";
	private final String filePathTipoUsuario = "codedata//tipoUsuario.txt";
	private final String filePathStatusUsuario = "codedata//statusUsuario.txt";
	private final String filePathCargo = "codedata//cargo.txt";
	private final String filePathStatusProposta = "codedata//statusProposta.txt";
	private final String filePathTipoCulinaria = "codedata//tipoCulinaria.txt";
	private final String filePathStatusRestaurante = "codedata//statusRestaurante.txt";
	
	public void loadCodedata(){
		try{
			LoadCountryEnvironment loadCountryEnvironment = new LoadCountryEnvironment();
			loadCountryEnvironment.loadCountry(filePathPais);
			
			LoadStateEnvironment loadStateEnvironment = new LoadStateEnvironment();
			loadStateEnvironment.loadState(filePathEstado);
			
			LoadCityEnvironment loadCityEnvironment = new LoadCityEnvironment();
			loadCityEnvironment.loadCity(filePathCidade);
			
			LoadUserTypeEnvironment loadUserTypeEnvironment = new LoadUserTypeEnvironment();
			loadUserTypeEnvironment.loadUserType(filePathTipoUsuario);
			
			LoadUserStatusEnvironment loadUserStatus = new LoadUserStatusEnvironment();
			loadUserStatus.loadUserStatus(filePathStatusUsuario);
			
			LoadEmploymentEnvironment loadEmployment = new LoadEmploymentEnvironment();
			loadEmployment.loadEmployment(filePathCargo);
			
			LoadProposalStatusEnvironment loadProposalStatus = new LoadProposalStatusEnvironment();
			loadProposalStatus.loadProposalStatus(filePathStatusProposta);
			
			LoadCulinaryTypeEnvironment loadCulinatyType = new LoadCulinaryTypeEnvironment();
			loadCulinatyType.loadCulinaryType(filePathTipoCulinaria);
			
			LoadRestaurantStatusEnvironment loadRestaurantStatusEnvironment = new LoadRestaurantStatusEnvironment();
			loadRestaurantStatusEnvironment.loadRestaurantStatus(filePathStatusRestaurante);
			
		}catch(Throwable e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		LoadCodedataParametersEnvironment l = new LoadCodedataParametersEnvironment();
		l.loadCodedata();
	}
}