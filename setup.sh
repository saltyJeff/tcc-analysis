cd ~
wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
~/bin/micromamba shell init bash
source ~/.bashrc
micromamba create -f SageMaker/environment.yml -y
micromamba activate spark_2_4_0
python -m ipykernel install --user --name=spark_2_4_0