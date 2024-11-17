# MoneyLineGANs

This project aims to develop and test a Generative Adversarial Network (GAN),
to generate synthetic moneyline betting data which accurately reflects the dynamics of the Massachusetts sports betting markets. By experimenting with
different model architectures, we hope to demonstrate that the generated syn thetic data can mimic real-world patterns and provide insights into market
inefficiencies and arbitrage opportunities.

This repo contains all the code for scarping real-time moneyline data from The Odds API. The scripts preprocess the raw data and stores it to AWS S3 with aggregated visualizations.

## Problem Formulation

Consider discrete measurements of moneyline data 

$$
\mathbf{x} = \begin{pmatrix}a\\
b\end{pmatrix} = \begin{pmatrix}\text{+ moneyline}\\
\text{- moneyline}\end{pmatrix}
$$

at the start of a game $S_0$. We have our dataset 

$$
D = \left\\{\mathbf{x}_i\right\\}\_{i=1}^N
$$ 

representing moneyline observations at $S_0$ for different NBA games. We want to estimate probability distributions for $\mathbf{x}_i$ such that

$$
\hat{p}\_\theta(x\_i) \approx p(\mathbf{x}_i).
$$

We can do this by finding 

$$
\min_{\hat{p}} D \left(p(\mathbf{x}_{i}) || \hat{p}\_\theta(x\_i)\right)
$$

using a Wasserstein-GAN where $D$ is our distance metric which is the Wasserstein-1 distance

$$
D = \inf_{\gamma \in \pi(\hat{p}_\theta(\mathbf{x_i}),p(\mathbf{x_i}))} \mathbb{E}\_{(x,y)\sim \gamma} [\\|x - y\\|]
$$

We will evaluate the performance of our generative model by comparing the similarity of the empirical distribution to the generated distribution 

$$\mathbf{x}\_{i_{g}} \sim \hat{p}_\theta(z_i)$$

where $z_i \sim N(0, I)$ is sampled from the multivariate Gaussian and passed through the generator network.

## Model Architecture 

The GAN consists of two neural networks: a generator that produces synthetic betting odds and a discriminator that evaluates whether the data is real or generated. By iteratively improving both networks, the generator learns to produce increasingly realistic betting odds. We use the W-GAN architecture which is known to be easier to train than traditional GANs. In addition, the original W-GAN implementation enforces a Lipschitz constraint on the weights matrix via parameter clipping which is known to have poor performance. We compare this original implementation to another W-GAN which enforces Lipschitz via spectral normalization.
