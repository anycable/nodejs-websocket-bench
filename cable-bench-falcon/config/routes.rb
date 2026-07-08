Rails.application.routes.draw do
  # Define your application routes per the DSL in https://guides.rubyonrails.org/routing.html

  # Reveal health status on /up that returns 200 if the app boots with no exceptions, otherwise 500.
  # Can be used by load balancers and uptime monitors to verify that the app is live.
  get "up" => "rails/health#show", as: :rails_health_check

  # Benchmark harness surface.
  get  "health"           => "bench#health"
  post "_bench/broadcast"  => "bench#broadcast"

  # WebSockets at /cable are handled by Async::Cable::Middleware (inserted by
  # the async-cable railtie), which intercepts the WS upgrade before the router
  # and dispatches to ActionCable on the Falcon reactor. No explicit mount.

  # Render dynamic PWA files from app/views/pwa/* (remember to link manifest in application.html.erb)
  # get "manifest" => "rails/pwa#manifest", as: :pwa_manifest
  # get "service-worker" => "rails/pwa#service_worker", as: :pwa_service_worker

  # Defines the root path route ("/")
  # root "posts#index"
end
